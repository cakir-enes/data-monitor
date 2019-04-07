package datamonitor

import mu.KotlinLogging


data class User(val name: String, val subscriptions: MutableList<String> = mutableListOf())
data class Topic(val name: String, val fields: Map<String, String>)

sealed class ServerStatus {
    object Success : ServerStatus()
    object UserNotFound : ServerStatus()
    object UsernameExists : ServerStatus()
    object TopicNotFound : ServerStatus()
}


object Server {
    private val streams: List<Stream> = listOf(NatsStream())
    private val users = mutableListOf<User>()
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

    fun subscribeUser(userName: String, topicName: String, topicHandler: (Topic) -> Unit): ServerStatus {
        logger.info { "SUBSCRIBE USER: $userName to $topicName" }
        val user = users.find { it.name == userName } ?: return ServerStatus.UserNotFound
        streams.forEach { it.subscribeToSubject(topicName, Topic::class.java, topicHandler) }
        return ServerStatus.Success
    }
}
