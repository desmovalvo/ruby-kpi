#!/usr/bin/ruby

# JOIN
JOIN_REQUEST_TEMPLATE = %{<SSAP_message>
	<node_id>%s</node_id>
	<space_id>%s</space_id>
	<transaction_type>JOIN</transaction_type>
	<message_type>REQUEST</message_type>
	<transaction_id>%s</transaction_id>
	<parameter name = "credentials">XXYYZZ</parameter>
</SSAP_message>}


# LEAVE 
LEAVE_REQUEST_TEMPLATE = %{<SSAP_message>
	<node_id>%s</node_id>
	<space_id>%s</space_id>
	<transaction_type>LEAVE</transaction_type>
	<message_type>REQUEST</message_type>
	<transaction_id>%s</transaction_id>
</SSAP_message>}


# INSERT
INSERT_REQUEST_TEMPLATE = %{<SSAP_message>
<node_id>%s</node_id>
<space_id>%s</space_id>
<transaction_type>INSERT</transaction_type>
<message_type>REQUEST</message_type>
<transaction_id>%s</transaction_id>
<parameter name = "insert_graph" encoding = "RDF-M3">
<triple_list>%s</triple_list></parameter>
<parameter name = "confirm">TRUE</parameter>
</SSAP_message>}

# REMOVE
REMOVE_REQUEST_TEMPLATE = %{<SSAP_message>
<message_type>REQUEST</message_type>
<transaction_type>REMOVE</transaction_type>
<transaction_id>%s</transaction_id>
<node_id>%s</node_id>
<space_id>%s</space_id>
<parameter name="confirm">TRUE</parameter>
<parameter name="remove_graph"  encoding="RDF-M3">
<triple_list>%s</triple_list></parameter>
</SSAP_message>}

# UPDATE
UPDATE_REQUEST_TEMPLATE = %{<SSAP_message>
<node_id>%s</node_id>
<space_id>%s</space_id>
<transaction_type>UPDATE</transaction_type>
<message_type>REQUEST</message_type>
<transaction_id>%s</transaction_id>

<parameter name = "insert_graph" encoding = "RDF-M3">
<triple_list>%s</triple_list>
</parameter>

<parameter name = "remove_graph" encoding = "RDF-M3">
<triple_list>%s</triple_list>
</parameter>

<parameter name = "confirm">TRUE</parameter>
</SSAP_message>}

# RDF QUERY
RDF_QUERY_REQUEST_TEMPLATE = %{<SSAP_message>
<node_id>%s</node_id>
<space_id>%s</space_id>
<transaction_type>QUERY</transaction_type>
<message_type>REQUEST</message_type>
<transaction_id>%s</transaction_id>
<parameter name = "type">RDF-M3</parameter>
<parameter name = "query">
<triple_list>%s</triple_list></parameter>
</SSAP_message>}

# SPARQL QUERY
SPARQL_QUERY_REQUEST_TEMPLATE = %{<SSAP_message>
<node_id>%s</node_id>
<space_id>%s</space_id>
<transaction_type>QUERY</transaction_type>
<message_type>REQUEST</message_type>
<transaction_id>%s</transaction_id>
<parameter name = "type">sparql</parameter>
<parameter name = "query">%s</parameter>
</SSAP_message>}

# RDF SUBSCRIBE
RDF_SUBSCRIBE_REQUEST_TEMPLATE = %{<SSAP_message>
<node_id>%s</node_id>
<space_id>%s</space_id>
<transaction_type>SUBSCRIBE</transaction_type>
<message_type>REQUEST</message_type>
<transaction_id>%s</transaction_id>
<parameter name = "type">RDF-M3</parameter>
<parameter name = "query">
<triple_list>%s</triple_list></parameter>
</SSAP_message>}

# SPARQL SUBSCRIBE
SPARQL_SUBSCRIBE_REQUEST_TEMPLATE = %{<SSAP_message>
<node_id>%s</node_id>
<space_id>%s</space_id>
<transaction_type>SUBSCRIBE</transaction_type>
<message_type>REQUEST</message_type>
<transaction_id>%s</transaction_id>
<parameter name = "type">sparql</parameter>
<parameter name = "query">%s</parameter>
</SSAP_message>}

# UNSUBSCRIBE
UNSUBSCRIBE_REQUEST_TEMPLATE = %{<SSAP_message>
<node_id>%s</node_id>
<space_id>%s</space_id>
<transaction_type>UNSUBSCRIBE</transaction_type>
<message_type>REQUEST</message_type>
<transaction_id>%s</transaction_id>
<parameter name = "subscription_id">%s</parameter>
</SSAP_message>}

# OTHER TEMPLATES
TRIPLE_TEMPLATE = %{<triple>
<subject type = "%s">%s</subject>
<predicate>%s</predicate>
<object type = "%s">%s</object>
</triple>}

