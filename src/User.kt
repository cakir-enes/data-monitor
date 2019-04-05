import io.nats.streaming.Subscription
import kotlinx.coroutines.channels.Channel

sealed class Status {
    object TopicNotFound : Status()
    object Success : Status()
}

val validtopics = listOf("deneme")

class User(private val name: String, private val subscriptions: MutableList<Subscription> = mutableListOf()) {
    val channel by lazy { Channel<String>() }

    fun subscribe(topic: String): Status {
        return if (validtopics.contains(topic)) {
            subscriptions.add(TopicStreamManager.subscribe(topic, channel))
            Status.Success
        } else {
            Status.TopicNotFound
        }
    }

    fun unsubscribe(topic: String): Status {
        val sub = subscriptions.find { it.subject == topic }
        return if (sub != null) {
            sub.unsubscribe()
            Status.Success
        } else {
            Status.TopicNotFound
        }
    }
}