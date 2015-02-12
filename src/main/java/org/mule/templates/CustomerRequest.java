/**
 * Mule Anypoint Template
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates;

import java.util.ArrayList;
import java.util.List;

import com.workday.revenue.CustomerRequestCriteriaType;
import com.workday.revenue.GetCustomersRequestType;

public class CustomerRequest {

	public static GetCustomersRequestType getCustomer(String id){
		GetCustomersRequestType get = new GetCustomersRequestType();
		List<CustomerRequestCriteriaType> requestCriteria = new ArrayList<CustomerRequestCriteriaType>();
		CustomerRequestCriteriaType crit = new CustomerRequestCriteriaType();
		crit.setCustomerReferenceID(id);
		requestCriteria.add(crit );
		get.setRequestCriteria(requestCriteria );
		return get ;
	}
}
