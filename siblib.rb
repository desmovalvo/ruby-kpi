#!/usr/bin/ruby

# requirements
require "xml"
require "uuid"
require "socket"
require "logger"
require "colorize"
load "TCPConnection.rb"
load "ssap_templates.rb"
load "Message.rb"

##################################################
#
# The Handler class
#
##################################################

class Handler

  # initializer
  def initialize()
    # nothing to do
  end

  # handle method
  def handle(added, removed)

    # print added triples
    puts "Added:"
    added.each do |a|
      if a.class == "Triple"
        puts a.to_str()
      else
        puts a.join(", ")
      end
    end
    
    # print removed triples
    puts "Removed:"
    removed.each do |r|
      if r.class == "Triple"
        puts r.to_str()
      else
        puts r.join(", ")
      end
    end

    return

  end

end


##################################################
#
# The Exceptions classes
#
##################################################

class SIBError < StandardError

end

##################################################
#
# The URI class
#
##################################################

class URI

  # accessor
  attr_reader :value

  # constructor
  def initialize(value)
    if value
      @value = value
    else
      @value = "http://www.nokia.com/NRC/M3/sib#any"
    end
  end

  # to string
  def to_str()
    return "<" + @value + ">"
  end

end


##################################################
#
# The Literal class
#
##################################################

class Literal

  # accessor
  attr_reader :value

  # constructor
  def initialize(value)
    @value = value
  end

  # to string
  def to_str()
    return @value
  end

end


##################################################
#
# The Triple class
#
##################################################

class Triple

  # accessor
  attr_reader :subject, :predicate, :object

  # constructor
  def initialize(subject, predicate, object)
    
    # subject
    @subject = subject

    # predicate
    @predicate = predicate

    # object
    @object = object

  end

  # to string
  def to_str()
    
    return "[" + @subject.to_str() + ", " + @predicate.to_str() + ", " + @object.to_str() + "]"

  end

  
end


##################################################
#
# The KP class
#
##################################################

class KP

  # accessors
  attr_reader :last_request, :last_reply, :active_subscriptions, :node_id, :ip, :port, :ss


  ####################################################
  #  
  # constructor 
  #
  ####################################################

  def initialize(ip, port, smart_space, debug = false)
    
    # debug object
    @debug = debug
    if @debug       
        @logger = Logger.new(STDOUT)
      @logger.debug("KP:initialize")
    end

    # instance variables
    @ip = ip
    @port = port
    @ss = smart_space
    @transaction_id = 1
    @node_id = UUID.new().generate() 
    @last_request = nil
    @last_reply = nil
    @active_subscriptions = {}

  end


  ####################################################
  #
  # join
  #
  ####################################################

  def join_sib()

    # debug print
    if @debug
        @logger.debug("KP:join_sib")
    end

    # building and storing the SSAP JOIN REQUEST
    msg = JOIN_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id ]
    @last_request = msg

    # connecting to the SIB
    tcpc = TCPConnection.new(@ip, @port)
    
    # sending the request  message
    tcpc.send_request(msg)

    # waiting for a reply
    rmsg = tcpc.receive_reply()

    # closing the socket
    tcpc.close()

    # storing last reply
    @last_reply = rmsg

    # reading message
    r = ReplyMessage.new(rmsg)

    # increment transaction id
    @transaction_id += 1

    # return
    return r.success?()
   
  end
  

  ####################################################
  #
  # LEAVE
  #
  ####################################################

  def leave_sib()

    # debug print
    if @debug
        @logger.debug("KP:leave_sib")
    end

    # building and storing the SSAP LEAVE REQUEST
    msg = LEAVE_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id ]
    @last_request = msg

    # connecting to the SIB
    tcpc = TCPConnection.new(@ip, @port)
    
    # sendind the request
    tcpc.send_request(msg)

    # waiting a reply
    rmsg = tcpc.receive_reply()

    ## instantiate a new ReplyMessage
    r = ReplyMessage.new(rmsg)

    # closing the socket
    tcpc.close()

    # storing last reply
    @last_reply = rmsg

    # increment transaction id
    @transaction_id += 1

    # return
    return r.success?()
    
  end



  ####################################################
  #
  # INSERT
  #
  ####################################################

  def insert(triple_list)

    # debug print
    if @debug
        @logger.debug("KP:insert")
    end

    # build the triple_string
    triple_string = ""
    triple_list.each do |triple|
      triple_string += TRIPLE_TEMPLATE % [triple.subject.class.to_s.downcase, triple.subject.value, triple.predicate.value, triple.object.class.to_s.downcase, triple.object.value]
    end

    # build the SSAP INSERT REQUEST
    msg = INSERT_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, triple_string ]

    # connecting to the SIB
    tcpc = TCPConnection.new(@ip, @port)
    
    # sendind the request
    tcpc.send_request(msg)

    # waiting a reply
    rmsg = tcpc.receive_reply()

    # closing the socket
    tcpc.close()

    # storing last reply
    @last_reply = rmsg

    # storing last request and last reply
    @last_request = msg
    @last_reply = rmsg

    # read the reply
    r = ReplyMessage.new(rmsg)

    # increment transaction id
    @transaction_id += 1

    # return
    return r.success?()
    
  end


  ####################################################
  #
  # REMOVE
  #
  ####################################################

  def remove(triple_list)

    # debug print
    if @debug
        @logger.debug("KP:remove")
    end

    # build the triple string
    triple_string = ""
    triple_list.each do |triple|
      triple_string += TRIPLE_TEMPLATE % [triple.subject.class, triple.subject.value, triple.predicate.value, triple.object.class, triple.object.value]
    end

    # building and storing the SSAP REMOVE REQUEST
    msg = REMOVE_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, triple_string ]
    @last_request = msg

    # connecting to the SIB
    tcpc = TCPConnection.new(@ip, @port)
    
    # sendind the request
    tcpc.send_request(msg)

    # waiting a reply
    rmsg = tcpc.receive_reply()

    # closing the socket
    tcpc.close()

    # storing last reply
    @last_reply = rmsg

    # increment transaction id
    @transaction_id += 1

    # parsing the message to get the return value
    r = ReplyMessage.new(rmsg)
    return r.success?()
    
  end


  ####################################################
  #
  # UPDATE
  #
  ####################################################

  def update(new_triple_list, old_triple_list)

    # debug print
    if @debug
        @logger.debug("KP:update")
    end

    # build the triple strings
    old_triple_string = ""
    old_triple_list.each do |triple|
      old_triple_string += TRIPLE_TEMPLATE % [triple.subject.class, triple.subject.value, triple.predicate.value, triple.object.class, triple.object.value]
    end
    new_triple_string = ""
    new_triple_list.each do |triple|
      new_triple_string += TRIPLE_TEMPLATE % [triple.subject.class, triple.subject.value, triple.predicate.value, triple.object.class, triple.object.value]
    end

    # building and storing the SSAP REMOVE REQUEST
    msg = UPDATE_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, new_triple_string, old_triple_string ]
    @last_request = msg

    # connecting to the SIB
    tcpc = TCPConnection.new(@ip, @port)
    
    # sendind the request
    tcpc.send_request(msg)

    # waiting a reply
    rmsg = tcpc.receive_reply()

    # closing the socket
    tcpc.close()

    # storing last reply
    @last_reply = rmsg

    # increment transaction id
    @transaction_id += 1

    # parsing the message to get the return value
    r = ReplyMessage.new(rmsg)
    return r.success?()
    
  end


  ####################################################
  #
  # RDF QUERY
  #
  ####################################################

  def rdf_query(triple)

    # debug print
    if @debug
        @logger.debug("KP:rdf_query")
    end

    # build the triple
    triple_string = TRIPLE_TEMPLATE % [triple.subject.class, triple.subject.value, triple.predicate.value, triple.object.class, triple.object.value]

    # build and store the SSAP QUERY REQUEST
    msg = RDF_QUERY_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, triple_string ]
    @last_request = msg

    # connecting to the SIB
    tcpc = TCPConnection.new(@ip, @port)
    
    # sendind the request
    tcpc.send_request(msg)

    # waiting a reply
    rmsg = tcpc.receive_reply()

    # storing and reading the reply
    @last_reply = rmsg
    r = ReplyMessage.new(rmsg)
    
    # closing the socket
    tcpc.close()

    # increment transaction id
    @transaction_id += 1

    # return
    return r.success?(), r.get_rdf_triples()

  end


  ####################################################
  #
  # SPARQL QUERY
  #
  ####################################################

  def sparql_query(q)

    # debug print
    if @debug
        @logger.debug("KP:sparql_query")
    end

    # build and store the SSAP SPARQL QUERY REQUEST
    q = q.gsub("<", "&lt;").gsub(">", "&gt;")
    msg = SPARQL_QUERY_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, q ]
    @last_request = msg
  
    # connecting to the SIB
    tcpc = TCPConnection.new(@ip, @port)
    
    # sendind the request
    tcpc.send_request(msg)

    # waiting a reply
    rmsg = tcpc.receive_reply()

    # closing the socket
    tcpc.close()

    # storing last reply
    @last_reply = rmsg

    # increment transaction id
    @transaction_id += 1
    
    # reading the reply
    r = ReplyMessage.new(rmsg)

    # return
    return r.success?(), r.get_sparql_results()
    
  end


  ####################################################
  #
  # RDF Subscription
  #
  ####################################################
  
  def rdf_subscribe(triple, myHandlerClass)

    # debug print
    if @debug
        @logger.debug("KP:rdf_subscribe")
    end

    # build the triple
    triple_string = TRIPLE_TEMPLATE % [triple.subject.class, triple.subject.value, triple.predicate.value, triple.object.class, triple.object.value]

    # build and store the SSAP QUERY REQUEST
    msg = RDF_SUBSCRIBE_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, triple_string ]
    @last_request = msg

    # connecting to the SIB
    tcpc = TCPConnection.new(@ip, @port)
    
    # sendind the request
    tcpc.send_request(msg)

    # waiting a reply
    rmsg = tcpc.receive_reply()

    # storing and reading the last reply
    @last_reply = rmsg
    r = ReplyMessage.new(rmsg)

    # increment transaction id
    @transaction_id += 1

    # get the subscription id
    subscription_id = r.get_subscription_id()

    # Get the initial results
    triple_list = r.get_rdf_triples()

    # instantiate the handler class
    h = myHandlerClass.new()

    # start the thread
    t = Thread.new{rdf_indication_receiver(tcpc, subscription_id, h)}    

    # store the subscription id, its socket and its thread
    @active_subscriptions[subscription_id] = {} 
    @active_subscriptions[subscription_id]["socket"] = tcpc
    @active_subscriptions[subscription_id]["thread"] = t

    # return
    return r.success?(), subscription_id, triple_list
    
  end


  ####################################################
  #
  # SPARQL Subscription
  #
  ####################################################
  
  def sparql_subscribe(pattern, myHandlerClass = nil)

    # debug print
    if @debug
        @logger.debug("KP:rdf_subscribe")
    end

    # build and store the SSAP QUERY REQUEST
    msg = SPARQL_SUBSCRIBE_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, pattern ]
    @last_request = msg

    # connecting to the SIB
    tcpc = TCPConnection.new(@ip, @port)
    
    # sendind the request
    tcpc.send_request(msg)

    # waiting a reply
    rmsg = tcpc.receive_reply()

    # storing and reading the last reply
    @last_reply = rmsg
    r = ReplyMessage.new(rmsg)

    # increment transaction id
    @transaction_id += 1

    # get the subscription id
    subscription_id = r.get_subscription_id()

    # Get the initial results
    initial_results = r.get_sparql_results()

    # start the indication receiver
    if myHandlerClass
      # TODO check if myHandlerClass is a Handler,
      # otherwise raise an exception
      h = myHandlerClass.new()
    else
      h = nil
    end
    t = Thread.new{sparql_indication_receiver(tcpc, subscription_id, h)}    

    # store the subscription id and its socket
    @active_subscriptions[subscription_id] = {} 
    @active_subscriptions[subscription_id]["socket"] = tcpc
    @active_subscriptions[subscription_id]["thread"] = t

    # return
    return r.success?(), subscription_id, initial_results
    
  end


  ####################################################
  #
  # unsubscribe
  #
  ####################################################
  
  def unsubscribe(sub_id)

    # debug print
    if @debug
        @logger.debug("KP:unsubscribe")
    end

    # building and storing the SSAP UNSUBSCRIBE REQUEST
    msg = UNSUBSCRIBE_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, sub_id ]
    @last_request = msg

    # connecting to the SIB
    tcpc = TCPConnection.new(@ip, @port)
    
    # sending the request  message
    tcpc.send_request(msg)

    # closing the socket
    tcpc.close()

    # increment transaction id
    @transaction_id += 1
   
    # get the thread and return
    return @active_subscriptions[sub_id]["thread"].value

  end


  ####################################################
  #
  # rdf indication receiver
  #
  ####################################################

  def rdf_indication_receiver(tcpc, subscription_id, handler)

    # debug print
    if @debug
        @logger.debug("KP:rdf_indication_receiver")
    end

    # Endless loop
    while true
    
      # receive
      rmsg = tcpc.receive_reply()
      r = ReplyMessage.new(rmsg)

      # is it an indication?
      if r.get_message_type() == "INDICATION"

        # debug print
        if @debug
          @logger.debug("KP:rdf_indication_receiver -- INDICATION")
        end

        # call the handler
        added, removed = r.get_rdf_triples_from_indication()
        handler.handle(added, removed)          
        
      # it is an unsubscribe confirm
      else

        # close subscription
        if r.success?()

          # debug print
          if @debug
            @logger.debug("KP:rdf_indication_receiver -- UNSUBSCRIBE CONFIRM")
          end

          # save the reply
          @last_reply = rmsg
              
          # close subscription
          @active_subscriptions[subscription_id]["socket"].close()
          t = @active_subscriptions[subscription_id]["thread"]
          @active_subscriptions.delete(subscription_id)
          
          # return 
          r = ReplyMessage.new(rmsg)
          return r.success?()
          
        end
      end 
    end    
  end

  
  ####################################################
  #
  # sparql indication receiver
  #
  ####################################################

  def sparql_indication_receiver(tcpc, subscription_id, handler)

    # debug print
    if @debug
        @logger.debug("KP:sparql_indication_receiver")
    end

    # Endless loop
    while true
    
      # receive
      rmsg = tcpc.receive_reply()
      r = ReplyMessage.new(rmsg)

      # parse the message
      content = XML::Parser.string(rmsg).parse
      
      # is it an indication?
      if r.get_message_type() == "INDICATION"

        # debug print
        if @debug
          @logger.debug("KP:sparql_indication_receiver -- INDICATION")
        end
        
        # extract triples from the indication and launch the handler
        added, removed = r.get_sparql_results_from_indication()
        handler.handle(added, removed)          
        
      # it is an unsubscribe confirm
      else

        # close subscription
        if r.success?()

          # debug print
          if @debug
            @logger.debug("KP:sparql_indication_receiver -- UNSUBSCRIBE CONFIRM")
          end
          
          # save the reply
          @last_reply = rmsg
          
          # close subscription
          @active_subscriptions[subscription_id]["socket"].close()
          t = @active_subscriptions[subscription_id]["thread"]
          @active_subscriptions.delete(subscription_id)

          # return 
          r = ReplyMessage.new(rmsg)
          return r.success?()
          
        end
      end 
    end
    
  end
  

end
