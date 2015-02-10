/**
 * Mule Anypoint Template
 * Copyright (c) MuleSoft, Inc.
 * All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mule.DefaultMuleMessage;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.construct.Flow;
import org.mule.context.notification.NotificationException;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;
import org.mule.templates.db.MySQLDbCreator;

import com.mulesoft.module.batch.BatchTestHelper;

/**
 * The objective of this class is to validate the correct behavior of the
 * Anypoint Template that make calls to external systems.
 * 
 */
@SuppressWarnings("unchecked")
public class BusinessLogicPushNotificationIT extends AbstractTemplateTestCase {
	
	private static final int TIMEOUT_MILLIS = 60;
	private static final String REQUEST_PATH = "./src/main/resources/accountExample.xml";
	private static final String PATH_TO_TEST_PROPERTIES = "./src/test/resources/mule.test.properties";
	private static final String PATH_TO_SQL_SCRIPT = "src/main/resources/account.sql";
	private static final String DATABASE_NAME = "SFDC2DBAccountBroadcast"
			+ new Long(new Date().getTime()).toString();
	private BatchTestHelper helper;
	private Flow triggerPushFlow;
	private SubflowInterceptingChainLifecycleWrapper selectAccountFromDBFlow;
	private List<String> accountsToDelete = new ArrayList<String>();
	private static final MySQLDbCreator DBCREATOR = new MySQLDbCreator(
			DATABASE_NAME, PATH_TO_SQL_SCRIPT, PATH_TO_TEST_PROPERTIES);
	
	@BeforeClass
	public static void beforeClass() {
		DBCREATOR.setUpDatabase();
		System.setProperty("database.url", DBCREATOR.getDatabaseUrlWithName());
		System.setProperty("trigger.policy", "push");
	}

	@AfterClass
	public static void shutDown() {
		System.clearProperty("trigger.policy");
		DBCREATOR.tearDownDataBase();
	}
	
	@Before
	public void setUp() throws Exception {
		System.setProperty("trigger.policy", "push");
		stopFlowSchedulers(POLL_FLOW_NAME);
		registerListeners();
		helper = new BatchTestHelper(muleContext);
		triggerPushFlow = getFlow("triggerPushFlow");
		selectAccountFromDBFlow = getSubFlow("selectAccountFromDB");
	}

	@After
	public void tearDown() throws Exception {
		stopFlowSchedulers(POLL_FLOW_NAME);
		deleteEntities();
	}

//	@Test
	public void testMainFlow() throws Exception {
		String accountName = buildUniqueName();
		MuleMessage message = new DefaultMuleMessage(buildRequest(accountName), muleContext);
		MuleEvent testEvent = getTestEvent(message, MessageExchangePattern.REQUEST_RESPONSE);
		triggerPushFlow.process(testEvent);
		
		helper.awaitJobTermination(TIMEOUT_MILLIS * 1000, 500);
		helper.assertJobWasSuccessful();

		HashMap<String, Object> account = new HashMap<String, Object>();
		account.put("Name", accountName);
		
		SubflowInterceptingChainLifecycleWrapper retrieveAccountFlow = getSubFlow("retrieveAccountFlow");
		retrieveAccountFlow.initialise();
		message = new DefaultMuleMessage(account, muleContext);
		testEvent = getTestEvent(message, MessageExchangePattern.REQUEST_RESPONSE);
		Map<String, String> accountInB = (Map<String, String>) retrieveAccountFlow.process(testEvent).getMessage().getPayload();
		
		assertNotNull(accountInB);
		assertEquals("Account Names should be equals", account.get("Name"), accountInB.get("Name"));
		accountsToDelete.add(accountInB.get("Id"));
		
		testEvent = selectAccountFromDBFlow.process(getTestEvent(
				account, MessageExchangePattern.REQUEST_RESPONSE));
		final List<Map<String, Object>> payloadDb = (List<Map<String, Object>>) testEvent
				.getMessage().getPayload();

		assertEquals("The account should have been sync to DB", 
				1, 
				payloadDb.size());
		
		assertEquals("The account SalesforceId in DB should match",
				account.get("Id"), 
				payloadDb.get(0).get("salesforceId"));
		
		assertEquals("The account name in DB should match",
				account.get("Name"), 
				payloadDb.get(0).get("name"));
	}
	
	private void deleteEntities() throws MuleException, Exception {
		SubflowInterceptingChainLifecycleWrapper deleteAccountFromBflow = getSubFlow("deleteAccountFromBFlow");
		deleteAccountFromBflow.initialise();
		deleteAccountFromBflow.process(getTestEvent(accountsToDelete, MessageExchangePattern.REQUEST_RESPONSE));
	}

	private void registerListeners() throws NotificationException {
		muleContext.registerListener(pipelineListener);
	}

	private String buildRequest(String accountName) throws IOException{
		byte[] encoded = Files.readAllBytes(Paths.get(REQUEST_PATH));
		return new String(encoded, "UTF-8").replaceAll("accountName", accountName);
	}
	
	private String buildUniqueName() {
		return TEMPLATE_NAME + "-" + System.currentTimeMillis() + "Account";
	}
	
}
