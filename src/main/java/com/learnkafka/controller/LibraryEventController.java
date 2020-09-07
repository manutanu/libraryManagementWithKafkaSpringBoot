package com.learnkafka.controller;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.KafkaProducerService;
import com.learnkafka.domain.LibraryEvent;

@RestController
public class LibraryEventController {
	
	@Autowired
	KafkaProducerService kafkaProducerService;
	
	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException{
		
		// kafkaProducerService.sendLibraryEvent(libraryEvent); // this method contains asynch code 
//		kafkaProducerService.sendLibraryEventSynchronous(libraryEvent); // this method is the sychronous way to publish message 
		kafkaProducerService.sendLibraryEventProducerRecordApproach(libraryEvent); // this method is asynch and uses ProducerRecord to construct message object
		
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}
}
