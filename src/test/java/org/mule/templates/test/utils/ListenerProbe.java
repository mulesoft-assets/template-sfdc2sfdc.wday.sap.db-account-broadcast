/**
 * Mule Anypoint Template
 * Copyright (c) MuleSoft, Inc.
 * All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.test.utils;

import org.mule.tck.probe.Probe;

/**
 * This {@link Probe} receives a {@link PipelineSynchronizeListener} as a parameter upon construction. It will check the readyToContinue method of the same to
 * validate if the Probe has been completed.
 * 
 * @author damiansima
 */
public class ListenerProbe implements Probe {
	private PipelineSynchronizeListener pipelineListener;

	public ListenerProbe(PipelineSynchronizeListener pipelineListener) {
		super();
		this.pipelineListener = pipelineListener;
	}

	@Override
	public boolean isSatisfied() {
		return pipelineListener.readyToContinue();
	}

	@Override
	public String describeFailure() {
		return "The listener never flaged that the notification of flow completion was received";
	}

}
