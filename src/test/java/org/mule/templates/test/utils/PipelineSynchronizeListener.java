/**
 * Mule Anypoint Template
 * Copyright (c) MuleSoft, Inc.
 * All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.test.utils;

import org.apache.commons.lang.Validate;
import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.context.notification.PipelineMessageNotificationListener;
import org.mule.context.notification.PipelineMessageNotification;

/**
 * This listener checks notifications of flows finalizations.
 * 
 * Upon notification it will check that the flowToCheck property matches the name of the flow notifying its completion. Should the name match it will change the
 * state of readyToContinue to true.
 * 
 * @author damiansima
 */
public class PipelineSynchronizeListener implements PipelineMessageNotificationListener<PipelineMessageNotification> {
	private String flowToCheck;
	private boolean readyToContinue;
	private Object notificatedPayload;

	public PipelineSynchronizeListener(String flowToCheck) {
		super();
		Validate.notEmpty(flowToCheck, "the name of the flow can not be null nor empty");

		this.flowToCheck = flowToCheck;
		this.readyToContinue = false;
	}

	@Override
	public void onNotification(PipelineMessageNotification notification) {
		MuleEvent event = ((MuleEvent) notification.getSource());
		MuleMessage message = event.getMessage();

		if (notification.getAction() == PipelineMessageNotification.PROCESS_END) {
			if (flowToCheck.equals(event.getFlowConstruct()
										.getName())) {
				readyToContinue = true;
				notificatedPayload = message.getPayload();
			}
		}
	}

	public boolean readyToContinue() {
		return readyToContinue;
	}

	public boolean resetListener() {
		return readyToContinue = false;
	}

	public Object getNotificatedPayload() {
		return notificatedPayload;
	}
}
