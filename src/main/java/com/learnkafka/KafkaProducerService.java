package com.learnkafka;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaProducerService {
	
	@Autowired
	KafkaTemplate<Integer , String > kafkaTemplate;
	
	@Autowired
	ObjectMapper objectMapper;
	
	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
		
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		ListenableFuture<SendResult<Integer , String >> listenableFuture = kafkaTemplate.sendDefault(key,value);
		
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer , String>>() {
			@Override
			public void onFailure(Throwable ex) {
				handleFailure(ex , key , value);
			}
			
			public void onSuccess(SendResult<Integer , String> result) {
				handleSuccess(result , key , value);
			};
		});
	}
	
	// this method sends message to kafka topic but uses ProducerRecord to construct message 
	public void sendLibraryEventProducerRecordApproach(LibraryEvent libraryEvent) throws JsonProcessingException {

		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		ProducerRecord<Integer,String> producerRecord = buildProducerRecord(key , value);
		
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);

		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
			@Override
			public void onFailure(Throwable ex) {
				handleFailure(ex, key, value);
			}

			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(result, key, value);
			};
		});
	}
	
	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
		
		List<Header> headers = Arrays.asList(
				new RecordHeader("event-source" , "barcode-scanner".getBytes()) ,
				new RecordHeader("target-topic" , "library-event-topic".getBytes())
				);
		return new ProducerRecord<Integer,String>("library-event-topic" , null , key , value , headers);
	}

	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {

		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);

		SendResult<Integer, String> sendResult = kafkaTemplate.sendDefault(key, value).get(2 , TimeUnit.SECONDS);
		
		return sendResult;
	}

	protected void handleFailure(Throwable ex, Integer key, String value) {
		log.error(ex.getLocalizedMessage());
		try {
			throw ex;
		}catch(Throwable e) {
			log.error(e.getLocalizedMessage());
		}
		
	}

	protected void handleSuccess(SendResult<Integer, String> result , Integer key , String value) {
		System.out.println(result + "  "+ value +"  "+ key);
		log.info(result.toString());
	}
}
