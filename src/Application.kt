package datamonitor

import User
import com.fasterxml.jackson.databind.SerializationFeature
import io.ktor.application.Application
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.features.ContentNegotiation
import io.ktor.features.StatusPages
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.cio.websocket.Frame
import io.ktor.jackson.jackson
import io.ktor.response.respond
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.post
import io.ktor.routing.route
import io.ktor.routing.routing
import io.ktor.websocket.webSocket
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.launch
import java.time.Duration


fun main(args: Array<String>) = io.ktor.server.netty.EngineMain.main(args)


@JvmOverloads
fun Application.module(testing: Boolean = false): Unit {
    val users = mutableMapOf<String, User>()
    install(io.ktor.websocket.WebSockets) {
        pingPeriod = Duration.ofSeconds(15)
        timeout = Duration.ofSeconds(15)
        maxFrameSize = Long.MAX_VALUE
        masking = false
    }

    install(ContentNegotiation) {
        jackson {
            enable(SerializationFeature.INDENT_OUTPUT)
        }
    }

    routing {
        install(StatusPages) {
            exception<AuthenticationException> { cause ->
                call.respond(HttpStatusCode.Unauthorized, "OLmadi hocam")
            }
            exception<AuthorizationException> { cause ->
                call.respond(HttpStatusCode.Forbidden)
            }

        }
        get("/") {
            call.respondText("HELLO WORLD!", contentType = ContentType.Text.Plain)
        }

        route("api") {

            get("/lol/{user}") {
                if (call.parameters["user"] == "sdf") throw AuthenticationException() else
                    call.respond("AL LAN" to "${call.parameters["user"]}")

            }

            post("/create/{user}") {
                val userName = call.parameters["user"]
                userName?.let {
                    users += it to User(it)
                    call.respondText("SUCCESS")
                } ?: call.respondText("Username cant be empty")
            }

            post("/{user}/subscribe/{topic}") {
                val user = call.parameters["user"]
                val topic = call.parameters["topic"]
                topic?.let {
                    users[user]?.subscribe(it)
                } ?: call.respondText("Topic can't be empty!")
            }

            webSocket("/subscriptions") {
                send(Frame.Text("Hi from server!"))
                val user = User("ABC")
                user.subscribe("deneme")
                launch { user.channel.consumeEach { send(Frame.Text(it)) } }
            }
        }
    }
}

class AuthenticationException : RuntimeException()
class AuthorizationException : RuntimeException()

