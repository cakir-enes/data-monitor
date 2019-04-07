import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.features.websocket.WebSockets
import io.ktor.client.features.websocket.ws
import io.ktor.client.request.post
import io.ktor.http.cio.websocket.Frame
import io.ktor.http.cio.websocket.readText
import io.nats.streaming.StreamingConnectionFactory
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.filterNotNull
import kotlinx.coroutines.channels.map
import kotlinx.coroutines.launch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.random.Random

fun main() {
    val client = HttpClient(CIO).config { install(WebSockets) }
    val connection by lazy {
        StreamingConnectionFactory(
            "test-cluster",
            Random.nextLong().toString()
        ).createConnection()
    }
    println("LOLOLO")
    GlobalScope.launch { client.post(host = "localhost", port = 8080, path = "/api/create/user/user1") }
    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate({
        connection.publish(
            "topic-foo",
            """{"name": "Oldu", "fields": { "birField": "22" } }""".toByteArray()
        )
    }, 0, 1000 / 10, TimeUnit.MILLISECONDS)
    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate({
        connection.publish(
            "topic-bar",
            """{"name": "Oldu", "fields": { "birField": "22" } }""".toByteArray()
        )
    }, 0, 1000 / 5, TimeUnit.MILLISECONDS)
    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate({
        connection.publish(
            "topic-baz",
            """{"name": "Oldu", "fields": { "birField": "22" } }""".toByteArray()
        )
    }, 0, 1000 / 1, TimeUnit.MILLISECONDS)

    GlobalScope.launch {
        client.ws(host = "127.0.0.1", port = 8080, path = "/api/subscription") {
            send(Frame.Text("user1/subscribe/topic-foo"))
            send(Frame.Text("user1/subscribe/topic-bar"))
            send(Frame.Text("user1/subscribe/topic-baz"))

            for (msg in incoming.map { it as? Frame.Text }.filterNotNull()) {
                println("MESSAGE ${msg.readText()}")
            }
        }
    }
}