package com.jsb.collector.kafka;

import org.springframework.beans.factory.annotation.Autowired;

//@RestController
public class KafkaController {

	private final Producer producer;

	@Autowired
	KafkaController(Producer producer) {
		this.producer = producer;
	}

//	@GetMapping("/example")
//	public String example(@RequestParam(value = "weapon", defaultValue = "pistol") String weapon) {
//		return "Example " + weapon;
//	}

//	@PostMapping(value = "/publish")
//	public void sendMessageToKafkaTopic(@RequestParam("message") String message) {
//		this.producer.sendMessage(message);
//	}
}
