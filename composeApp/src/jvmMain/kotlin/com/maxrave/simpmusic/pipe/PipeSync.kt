/*
 * Pipe Música PC — personal fork of SimpMusic (GPL-3.0)
 */

package com.maxrave.simpmusic.pipe

import com.maxrave.domain.data.entities.DownloadState
import com.maxrave.domain.data.model.browse.album.Track
import com.maxrave.domain.manager.DataStoreManager
import com.maxrave.domain.mediaservice.handler.DownloadHandler
import com.maxrave.domain.repository.PlaylistRepository
import com.maxrave.domain.repository.SongRepository
import com.maxrave.domain.utils.Resource
import com.maxrave.domain.utils.toSongEntity
import com.maxrave.logger.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.jetbrains.compose.resources.getString
import org.koin.mp.KoinPlatform.getKoin
import simpmusic.composeapp.generated.resources.Res
import simpmusic.composeapp.generated.resources.view_count
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Keeps Pipe Música PC in step with the phone through the user's YouTube Music account.
 *
 * - Likes: a three-way merge between YouTube's "Liked music" (LM) playlist, the local Favorites
 *   and the set both sides agreed on after the last pass. A like or unlike made on either side
 *   reaches the other one; unliking never deletes a download.
 * - Downloads: every liked song (when "Download songs you like" is on) and every song in the
 *   YouTube playlist named [DOWNLOAD_PLAYLIST_TITLE] is downloaded on this computer.
 *
 * Playlists and recommendations need nothing here: they live in the YouTube account and the app
 * already reads them from there once you are signed in.
 */
object PipeSync {
    private const val TAG = "PipeSync"

    /** A YouTube playlist with this name is kept downloaded on every device. */
    const val DOWNLOAD_PLAYLIST_TITLE = "Descargas Pipe"

    private const val LIKED_PLAYLIST_ID = "LM"
    private const val BASE_KEY = "pipe_liked_sync_base"
    private const val FIRST_RUN_KEY = "pipe_first_run_done"
    private const val PARALLEL_DOWNLOADS = 3

    private val INTERVAL = 30.minutes
    private val LOCAL_CHANGE_DEBOUNCE = 15.seconds

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val syncLock = Mutex()
    private val downloadPermits = Semaphore(PARALLEL_DOWNLOADS)
    private val inFlight: MutableSet<String> = ConcurrentHashMap.newKeySet()

    private val dataStore: DataStoreManager get() = getKoin().get()
    private val songRepository: SongRepository get() = getKoin().get()
    private val playlistRepository: PlaylistRepository get() = getKoin().get()
    private val downloadHandler: DownloadHandler get() = getKoin().get()

    /** Call once, after Koin has started. */
    @OptIn(FlowPreview::class)
    fun start() {
        scope.launch {
            applyFirstRunDefaults()
            while (isActive) {
                syncNow()
                delay(INTERVAL)
            }
        }
        // A like or unlike made on this computer reaches YouTube within seconds, not at the next tick.
        scope.launch {
            songRepository
                .getLikedSongs()
                .map { songs -> songs.map { it.videoId }.toSet() }
                .distinctUntilChanged()
                .drop(1)
                .debounce(LOCAL_CHANGE_DEBOUNCE)
                .collect { syncNow() }
        }
        // Signing in starts a pass straight away.
        scope.launch {
            dataStore.loggedIn
                .distinctUntilChanged()
                .drop(1)
                .collect { if (it == DataStoreManager.TRUE) syncNow() }
        }
    }

    suspend fun syncNow() {
        if (!syncLock.tryLock()) return
        try {
            if (dataStore.loggedIn.first() != DataStoreManager.TRUE) {
                Logger.d(TAG, "Not signed in, nothing to sync")
                return
            }
            val viewString = getString(Res.string.view_count)
            runStep("likes") { syncLikes(viewString) }
            runStep("liked downloads") { downloadLikedSongs() }
            runStep("download playlist") { syncDownloadPlaylist(viewString) }
        } finally {
            syncLock.unlock()
        }
    }

    private suspend fun applyFirstRunDefaults() {
        runStep("first run") {
            if (dataStore.getString(FIRST_RUN_KEY).first() == DataStoreManager.TRUE) return@runStep
            dataStore.setThemeMode(DataStoreManager.THEME_MODE_LIGHT)
            dataStore.setAutoDownloadLikedSongs(true)
            dataStore.putString(FIRST_RUN_KEY, DataStoreManager.TRUE)
        }
    }

    private suspend fun syncLikes(viewString: String) {
        val remoteTracks = fetchPlaylistTracks(LIKED_PLAYLIST_ID, viewString) ?: return
        val remote = remoteTracks.map { it.videoId }.toSet()
        val local = songRepository.getLikedSongs().first().map { it.videoId }.toSet()
        val base =
            dataStore
                .getString(BASE_KEY)
                .first()
                ?.split(',')
                ?.filter { it.isNotBlank() }
                ?.toSet()
                .orEmpty()

        // A continuation that failed half way returns a short list; reading that as "the user
        // unliked hundreds of songs" would undo them here, so removals wait for a full answer.
        val remoteLooksComplete = base.isEmpty() || remote.size * 2 >= base.size

        val remoteAdded = remote - base - local
        val remoteRemoved = if (remoteLooksComplete) (base - remote) intersect local else emptySet()
        val localAdded = local - base - remote
        val localRemoved = (base - local) intersect remote

        val pending = mutableSetOf<String>()
        remoteTracks.filter { it.videoId in remoteAdded }.forEach { track ->
            if (!likeLocally(track)) pending += track.videoId
        }
        remoteRemoved.forEach { songRepository.updateLikeStatus(it, 0) }
        localAdded.forEach { id -> songRepository.addToYouTubeLiked(id).firstOrNull() }
        localRemoved.forEach { id -> songRepository.removeFromYouTubeLiked(id).firstOrNull() }

        var agreed = (remote + localAdded) - localRemoved - pending
        if (!remoteLooksComplete) agreed = agreed + (base intersect local)
        dataStore.putString(BASE_KEY, agreed.joinToString(","))
        Logger.d(
            TAG,
            "Likes: +${remoteAdded.size}/-${remoteRemoved.size} from YouTube, " +
                "+${localAdded.size}/-${localRemoved.size} to YouTube",
        )
    }

    /**
     * Likes [track] on this computer. A song new to the database is inserted already liked, which
     * applies at once; an existing one goes through the repository, which also starts its download
     * when "Download songs you like" is on, so it runs in the download queue.
     *
     * @return true when the like is already applied.
     */
    private suspend fun likeLocally(track: Track): Boolean {
        val existing = songRepository.getSongById(track.videoId).first()
        if (existing == null) {
            songRepository.insertSong(track.toSongEntity().copy(liked = true)).firstOrNull()
            return true
        }
        if (existing.liked) return true
        queue(track.videoId) { songRepository.updateLikeStatus(track.videoId, 1) }
        return false
    }

    private suspend fun downloadLikedSongs() {
        if (dataStore.autoDownloadLikedSongs.first() != DataStoreManager.TRUE) return
        songRepository
            .getLikedSongs()
            .first()
            .filter { it.downloadState == DownloadState.STATE_NOT_DOWNLOADED }
            .forEach { song -> queueDownload(song.videoId, song.title, song.thumbnails.orEmpty()) }
    }

    private suspend fun syncDownloadPlaylist(viewString: String) {
        val playlists = playlistRepository.getLibraryPlaylist().first().orEmpty()
        val targets = playlists.filter { it.title.trim().equals(DOWNLOAD_PLAYLIST_TITLE, ignoreCase = true) }
        if (targets.isEmpty()) {
            Logger.d(TAG, "No \"$DOWNLOAD_PLAYLIST_TITLE\" playlist in this account")
            return
        }
        targets.forEach { playlist ->
            val tracks = fetchPlaylistTracks(playlist.browseId, viewString) ?: return@forEach
            tracks.forEach { track ->
                val song =
                    songRepository.getSongById(track.videoId).first()
                        ?: run {
                            songRepository.insertSong(track.toSongEntity()).firstOrNull()
                            songRepository.getSongById(track.videoId).first()
                        }
                if (song != null && song.downloadState == DownloadState.STATE_NOT_DOWNLOADED) {
                    queueDownload(song.videoId, song.title, song.thumbnails.orEmpty())
                }
            }
            Logger.d(TAG, "\"${playlist.title}\": ${tracks.size} songs checked")
        }
    }

    private suspend fun fetchPlaylistTracks(
        playlistId: String,
        viewString: String,
    ): List<Track>? =
        when (val result = playlistRepository.getFullPlaylistData(playlistId, viewString).first()) {
            is Resource.Success -> result.data?.tracks
            else -> {
                Logger.w(TAG, "Could not read playlist $playlistId: ${result.message}")
                null
            }
        }

    private fun queueDownload(
        videoId: String,
        title: String,
        thumbnail: String,
    ) {
        if (downloadHandler.downloadTask.value[videoId] == DownloadState.STATE_DOWNLOADING) return
        queue(videoId) { downloadHandler.downloadTrack(videoId, title, thumbnail) }
    }

    /** Runs [block] for [videoId] in the download queue, at most once at a time per song. */
    private fun queue(
        videoId: String,
        block: suspend () -> Unit,
    ) {
        if (!inFlight.add(videoId)) return
        scope.launch {
            try {
                downloadPermits.withPermit { block() }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                Logger.e(TAG, "Queued work for $videoId failed: ${e.message}")
            } finally {
                inFlight.remove(videoId)
            }
        }
    }

    private suspend fun runStep(
        name: String,
        block: suspend () -> Unit,
    ) {
        try {
            block()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Logger.e(TAG, "Step \"$name\" failed: ${e.message}")
        }
    }
}
