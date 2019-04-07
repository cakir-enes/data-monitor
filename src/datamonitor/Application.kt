package datamonitor

import com.fasterxml.jackson.databind.SerializationFeature
import io.ktor.application.Application
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.application.log
import io.ktor.features.ContentNegotiation
import io.ktor.features.StatusPages
import io.ktor.http.HttpStatusCode
import io.ktor.http.cio.websocket.Frame
import io.ktor.http.cio.websocket.readText
import io.ktor.jackson.jackson
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.post
import io.ktor.routing.route
import io.ktor.routing.routing
import io.ktor.websocket.webSocket
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.mapNotNull
import kotlinx.coroutines.launch
import java.time.Duration


fun main(args: Array<String>) = io.ktor.server.netty.EngineMain.main(args)

@kotlinx.serialization.ImplicitReflectionSerializer
@JvmOverloads
fun Application.module(testing: Boolean = false): Unit {
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
                call.respond(HttpStatusCode.Unauthorized, "Olmadi hocam")
            }
            exception<AuthorizationException> { cause ->
                call.respond(HttpStatusCode.Forbidden)
            }

        }

        route("api") {

            get("/lol/{user}") {
                if (call.parameters["user"] == "sdf") throw AuthenticationException() else
                    call.respond("AL LAN" to "${call.parameters["user"]}")
            }

            post("/create/user/{user}") {
                val userName = call.parameters["user"]

                when (Server.addUser(userName!!)) {
                    ServerStatus.Success -> call.respondText { "SUCCESS" }
                    ServerStatus.UsernameExists -> call.respondText { "Pick Different username" }
                }
            }
            post("/{user}/publish") {
                val userName = call.parameters["user"]!!
                val topic = call.receive<Topic>()
                Server.publish(userName, topic)
                call.respond(HttpStatusCode.OK)
            }

            webSocket("/subscription") {
                incoming
                    .mapNotNull { it as? Frame.Text }
                    .consumeEach { frame ->
                        val (user, op, topic) = frame.readText().split("/")
                        log.debug("WS RECIEVED ${frame.readText()}")
                        val status = when (op) {
                            "subscribe" -> Server.subscribeUser(user, topic) { topic ->
                                launch { outgoing.send(Frame.Text(topic.toString())) }
                            }
                            "unsubscribe" -> Server.unsubscribeUser(user, topic)
                            else -> null
                        }
                        val response = when (status) {
                            is ServerStatus.Success -> "OK"
                            is ServerStatus.UserNotFound -> "USER_NOT_FOUND"
                            is ServerStatus.TopicNotFound -> "TOPIC_NOT_FOUND"
                            else -> "UNKNOWN_OPERATION"
                        }
                        outgoing.send(Frame.Text(response))
                    }
            }
        }
    }
}

class AuthenticationException : RuntimeException()
class AuthorizationException : RuntimeException()

