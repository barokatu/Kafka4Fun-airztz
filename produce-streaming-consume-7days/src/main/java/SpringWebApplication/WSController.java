package SpringWebApplication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

@Controller
public class WSController {
	private static SimpMessagingTemplate template;

	@Autowired
	WSController(SimpMessagingTemplate template) {
		WSController.template = template;
	}

	public static SimpMessagingTemplate getTemplate() {
		return template;
	}
	// triggered when message was sent to websocket
	// @SendTo("/topic/random")
	// public double produce(String message) throws InterruptedException {
	// Thread.sleep(1000); // simulated delay
	// return Math.random()+10;
	// }
	// triggered when topic was subscribed
	// @SubscribeMapping("/topic/random")
	// public double feeback() throws InterruptedException {
	// Thread.sleep(1000); // simulated delay
	// return Math.random()+10;
	// }

}
