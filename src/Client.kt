import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import datamonitor.Topic
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.features.websocket.WebSockets
import io.ktor.client.features.websocket.ws
import io.ktor.client.request.post
import io.ktor.content.TextContent
import io.ktor.http.ContentType
import io.ktor.http.cio.websocket.Frame
import io.ktor.http.cio.websocket.readText
import io.nats.streaming.StreamingConnectionFactory
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.filterNotNull
import kotlinx.coroutines.channels.map
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.random.Random

fun main() {
    val client = HttpClient(CIO).config {
        install(WebSockets)
    }
    val connection by lazy {
        StreamingConnectionFactory(
            "test-cluster",
            Random.nextLong().toString()
        ).createConnection()
    }
    println("LOLOLO")
    var x1 = 0
    var x2 = 0
    var x3 = 0
    val JSON = jacksonObjectMapper()
    GlobalScope.launch {
        client.post<Unit>(host = "localhost", port = 8080, path = "/api/create/user/user1")
    }
    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate({
        val topic = """{"name": "10HZ", "fields": { "birField": "${x1++}" } }"""
//        connection.publish(
//            "topic-foo",
//            """{"name": "10HZ", "fields": { "birField": "${x1++}" } }""".toByteArray()
//        )
        GlobalScope.launch {
            client.post<Unit>(host = "localhost", port = 8080, path = "/api/user1/publish") {
                body = TextContent(topic, ContentType.Application.Json)
            }
        }
    }, 0, 1000 / 10, TimeUnit.MILLISECONDS)
//    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate({
//        connection.publish(
//            "topic-bar",
//            """{"name": "5 HZ", "fields": { "birField": "${x2++}" } }""".toByteArray()
//        )
//    }, 0, 1000 / 5, TimeUnit.MILLISECONDS)
//    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate({
//        connection.publish(
//            "topic-baz",
//            """{"name": "1HZ", "fields": { "birField": "${x3++}" } }""".toByteArray()
//        )
//    }, 0, 1000 / 1, TimeUnit.MILLISECONDS)

    GlobalScope.launch {
        client.ws(host = "127.0.0.1", port = 8080, path = "/api/subscription") {
            send(Frame.Text("user1/subscribe/topic-foo"))

//            send(Frame.Text("user1/subscribe/topic-bar"))
//            send(Frame.Text("user1/subscribe/topic-baz"))
            launch { delay(2000); send(Frame.Text("user1/publish/topic-foo")) }
            for (msg in incoming.map { it as? Frame.Text }.filterNotNull()) {
                println("MESSAGE ${msg.readText()}")
            }
        }
//        val nc = NatsStream()

//            .also { delay(2000); it.unsubscribe() }
//        nc.subscribe("topic-bar", Topic::class.java) { println("Hooppa $it") }
//        nc.subscribe("topic-baz", Topic::class.java) { println("Hooppa $it") }
//        println("SDF")
    }
    connection.subscribe("10HZ") { println("Hooppa ${JSON.readValue<Topic>(it.data)}") }
    Thread.sleep(20_000)
}