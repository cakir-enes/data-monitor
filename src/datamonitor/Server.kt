package datamonitor

import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.consumeEach
import mu.KotlinLogging


data class User(val name: String, val subscriptions: MutableList<String> = mutableListOf())

sealed class ServerStatus {
    object Success : ServerStatus()
    object UserNotFound : ServerStatus()
    object UsernameExists : ServerStatus()
    object TopicNotFound : ServerStatus()
}


object Server {
    private val streams: List<Stream> = listOf(NatsStream())
    private val users = mutableListOf<User>()
    private val topicChannels = mutableMapOf<String, List<BroadcastChannel<Topic>>>()
    private val logger = KotlinLogging.logger { }

    fun addUser(name: String): ServerStatus {
        logger.info { "ADDING USER $name" }
        return when (users.find { it.name == name }) {
            null -> {
                users.add(User(name)); ServerStatus.Success
            }
            else -> ServerStatus.UsernameExists
        }
    }

    suspend fun subscribeUser(userName: String, topicName: String, topicHandler: (Topic) -> Unit): ServerStatus {
        logger.info { "SUBSCRIBE USER: $userName to $topicName" }
        val user = users.find { it.name == userName } ?: return ServerStatus.UserNotFound
        val channels = topicChannels.computeIfAbsent(topicName) {
            streams.mapNotNull { stream -> stream.createChannelFor(topicName) }
        }
        user.subscriptions.add(topicName)
        channels.forEach { it.openSubscription().consumeEach(topicHandler) }
        return ServerStatus.Success
    }
}
