package datamonitor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.nats.streaming.StreamingConnectionFactory
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.broadcast
import kotlinx.coroutines.launch
import mu.KotlinLogging


data class Topic(val name: String, val fields: Map<String, String>)


interface Stream {
    val name: String
    val availableTopics: List<String>
    fun createChannelFor(topicName: String): BroadcastChannel<Topic>?
}


class NatsStream : Stream {

    private val connection by lazy { StreamingConnectionFactory("test-cluster", "bar").createConnection() }
    private val json = jacksonObjectMapper()
    private val logger = KotlinLogging.logger { }
    override val name = "NATS"
    override val availableTopics: List<String>
        get() = listOf("topic-foo", "topic-bar", "topic-baz")

    override fun createChannelFor(topicName: String): BroadcastChannel<Topic>? {
        if (!availableTopics.contains(topicName))
            return null
        val channel = Channel<Topic>().broadcast()
        connection.subscribe(topicName) {
            CoroutineScope(Dispatchers.IO).launch {
                val topic: Topic = json.readValue(it.data)
                println("RECEIVED")
                logger.debug { "$name Received $topic " }
                channel.send(topic)
            }
        }
        return channel
    }
}