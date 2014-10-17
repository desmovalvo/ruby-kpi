#!/usr/bin/ruby

# requirements
require "xml"

class ReplyMessage

  attr_reader :content, :message, :message_type, :transaction_type

  # initialization
  def initialize(message)

    # attributes needed to parse the message
    @message = message
    @content = nil

    # message attributes
    @transaction_type = nil
    @message_type = nil

    # parse the message
    @content = XML::Parser.string(@message).parse

  end

  # get_message_type
  def get_message_type()
      
    # get the message type
    @message_type = @content.find('//message_type').first.content()
    return @message_type
    
  end

  # get_transaction_type
  def get_transaction_type()
      
    # get the transaction type
    @transaction_type = @content.find('//transaction_type').first.content()
    return @transaction_type

  end

  # get_status
  def success?()

    # find the new root
    pars = @content.root.find('./parameter')
    pars.each do |p|
      
      # get and return the status
      if p.attributes.get_attribute("name").value == "status"
        return p.content == "m3:Success" ? true : false
        break
      end 
    end
  end

  # get_rdf_triples
  def get_rdf_triples()
    # yet to implement
  end

  # get_sparql_results
  def get_sparql_results()
    # yet to implement
  end

  # get_rdf_initial_triples
  def get_rdf_initial_triples()
    # yet to implement
  end

  # get_sparql_initial_results
  def get_sparql_initial_results()
    # yet to implement
  end

end
