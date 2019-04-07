package datamonitor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.nats.streaming.StreamingConnectionFactory
import mu.KotlinLogging
import java.util.concurrent.TimeoutException


interface Subscription {
    val subject: String
    fun unsubscribe()
}

interface Stream {

    class ConnectionFailed : Exception()
    class SubjectNotFound(msg: String) : Exception(msg)

    val name: String

    @Throws(ConnectionFailed::class, SubjectNotFound::class)
    fun <T> subscribe(subject: String, type: Class<T>, handler: (T) -> Unit): Subscription
}

class NatsStream : Stream {

    private val connection by lazy { StreamingConnectionFactory("test-cluster", "bar").createConnection() }
    private val JSON = jacksonObjectMapper()
    private val logger = KotlinLogging.logger { }
    private val subjects = listOf("topic-foo", "topic-bar", "topic-baz")
    override val name = "NATS"

    override fun <T> subscribe(subject: String, type: Class<T>, handler: (T) -> Unit): Subscription {
        logger.info { "Subscribing to $subject" }

        if (!subjects.contains(subject)) throw Stream.SubjectNotFound("$subject Not Found")

        try {
            val sub = connection.subscribe(subject) {
                val msg = JSON.readValue(it.data, type)
                handler(msg)
            }
            return object : Subscription {
                override val subject = subject
                override fun unsubscribe() {
                    sub.unsubscribe()
                }
            }
        } catch (e: TimeoutException) {
            throw Stream.ConnectionFailed()
        } catch (e: Exception) {
            throw e
        }
    }
}