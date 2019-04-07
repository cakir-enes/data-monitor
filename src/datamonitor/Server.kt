package datamonitor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import mu.KotlinLogging

data class User(val name: String, val subscriptions: MutableList<Subscription> = mutableListOf())
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
    private val JSON = jacksonObjectMapper()

    fun addUser(name: String): ServerStatus {
        logger.info { "ADDING USER $name" }
        val user = users.find { it.name == name }
        return if (user != null) {
            ServerStatus.UsernameExists
        } else {
            users.add(User(name))
            ServerStatus.Success
        }
    }

    fun subscribeUser(userName: String, topicName: String, topicHandler: (Topic) -> Unit): ServerStatus {
        logger.info { "SUBSCRIBE USER: $userName to $topicName" }
        val user = users.find { it.name == userName } ?: return ServerStatus.UserNotFound
        streams
            .map { stream -> stream.subscribe(topicName, Topic::class.java, topicHandler) }
            .forEach { sub -> user.subscriptions.add(sub) }
        return ServerStatus.Success
    }

    fun unsubscribeUser(userName: String, topicName: String): ServerStatus {
        val user = users.find { it.name == userName } ?: return ServerStatus.UserNotFound
        val subscription = user?.subscriptions?.find { it.subject == topicName } ?: return ServerStatus.TopicNotFound
        subscription?.unsubscribe()
        return ServerStatus.Success
    }

    fun publish(userName: String, topic: Topic) {
//        val topic = JSON.readValue<Topic>(topicJSON)
        streams.forEach { it.publish(topic.name, topic, Topic::class.java) }
    }
}
