package com.example.demo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class HelloController {

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@GetMapping("/test/{name}")
	public String testAPI(@PathVariable("name") String name) {
		Person p = Person.builder().id(1L).name(name).build();

		rabbitTemplate.convertAndSend("Mobile", p);
		rabbitTemplate.convertAndSend("Direct-Exchange", p);
		rabbitTemplate.convertAndSend("Fanout-Exchange", p);
		rabbitTemplate.convertAndSend("Topic-Exchange",p);

		return "SUCCESS";
	}
	
	
	@GetMapping("/test/new/{name}")
	public String testnewAPI(@PathVariable("name") String name) throws IOException {
		Person p = new Person(1L, name);
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = new ObjectOutputStream(bos);
		out.writeObject(p);
		out.flush();
		out.close();
		
		byte[] byteMessage = bos.toByteArray();
		bos.close();
		
		Message message = MessageBuilder.withBody(byteMessage)
				.setHeader("item1", "mobile")
				.setHeader("item2", "television").build();
		
		rabbitTemplate.send("Header-Exchange", "", message);
		
		return "Success";
	}
}
