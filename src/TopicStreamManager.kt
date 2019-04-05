import io.nats.streaming.StreamingConnectionFactory
import io.nats.streaming.Subscription
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.launch
import mu.KotlinLogging

object TopicStreamManager {
    private val connectionFactory = StreamingConnectionFactory("test-cluster", "bar")
    private val streamingConnection = connectionFactory.createConnection()
    private val logger = KotlinLogging.logger { }

    fun subscribe(topicName: String, channel: SendChannel<String>): Subscription {
        return streamingConnection.subscribe(topicName) {
            val msg = it.data
            logger.debug { "MESSAGE RECEIVED ${String(msg)}." }
            GlobalScope.launch { channel.send(String(msg)) }
        }
    }
}