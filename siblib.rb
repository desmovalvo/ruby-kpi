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
  def init()
    # nothing to do
    puts "handler instantiated".yellow
  end

  # handle method
  def handle(added, removed)
    puts "handle".yellow

    # print added triples
    puts "Added:"
    added.each do |a|
      puts a.to_str()
    end
    
    # print removed triples
    puts "Removed:"
    removed.each do |r|
      puts r.to_str()
    end

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

    # increment transaction id
    @transaction_id += 1

    # parsing the message to get the return value
    content = XML::Parser.string(rmsg).parse
    pars = content.root.find('./parameter')
    pars.each do |p|
      if p.attributes.get_attribute("name").value == "status"
        return p.content == "m3:Success" ? true : false
        break
      end 
    end
    
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
    content = XML::Parser.string(rmsg).parse
    pars = content.root.find('./parameter')
    pars.each do |p|
      if p.attributes.get_attribute("name").value == "status"
        return p.content == "m3:Success" ? true : false
        break
      end 
    end
    
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

    # closing the socket
    tcpc.close()

    # storing last reply
    @last_reply = rmsg

    # increment transaction id
    @transaction_id += 1

    # parsing the message to get the return value
    content = XML::Parser.string(rmsg).parse
    pars = content.root.find('./parameter')
    return_value = nil
    pars.each do |p|
      if p.attributes.get_attribute("name").value == "status"
        return_value = p.content == "m3:Success" ? true : false
        break
      end 
    end

    # Get the triple list
    triple_list = []
    content = XML::Parser.string(rmsg).parse
    pars = content.root.find('./parameter')
    pars.each do |p|

      if p.attributes.get_attribute("name").value == "results"

        # new root
        nroot = p
        t = p.find('./triple_list').first.find('./triple')
        t.each do |tr|

          # get subject
          s = tr.find('./subject').first
          s_content = s.content.strip
          s_type = s.attributes.get_attribute("type").value.strip
          if s_type.downcase == "uri"
            subject = URI.new(s_content)
          else
            subject = Literal.new(s_content)
          end
    
          # get predicate
          p = tr.find('./predicate').first
          p_content = p.content.strip
          predicate = URI.new(p_content)
    
          # get object
          o = tr.find('./object').first
          o_content = o.content.strip
          o_type = o.attributes.get_attribute("type").value.strip
          if o_type.downcase == "uri"
            object = URI.new(o_content)
          else
            object = Literal.new(o_content)
          end

          # triple
          t = Triple.new(subject, predicate, object)
          triple_list << t
    
        end
      end 
    end
    
    return return_value, triple_list
    
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

    # parsing the message to get the return value
    content = XML::Parser.string(rmsg).parse
    pars = content.root.find('./parameter')
    return_value = nil
    pars.each do |p|
      if p.attributes.get_attribute("name").value == "status"
        return_value = p.content == "m3:Success" ? true : false
        break
      end 
    end
    
    # find query results
    results = []
    
    # Get the triple list
    content = XML::Parser.string(rmsg.strip).parse
    pars = content.root.find('./parameter')
    pars.each do |p|
      if p.attributes.get_attribute("name").value == "results"

        # we're on the sparql node
        p.each_element do |sparql|
    
          sparql.each_element do |hr|
          
            # head/results fields
            hr.each_element do |field|
                
              # find the results
              if field.name == "result"
    
                # We found a result
                result = []
                
                field.each_element do |n|
                  variable = []
                  variable << n.attributes.first.value
                  n.each_element do |v|
                    variable << v.name
                    variable << v.content
                  end
                  result << variable
                  results << result
                end

              end
            end        
          end
          break
        end
      end
    end

    return return_value, results
    
  end


  ####################################################
  #
  # RDF Subscription
  #
  ####################################################
  
  def rdf_subscribe(triple, myHandlerClass = nil)

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

    puts 'request sent'
    
    # waiting a reply
    rmsg = tcpc.receive_reply()

    # storing last reply
    @last_reply = rmsg

    # increment transaction id
    @transaction_id += 1

    # parsing the message to get the return value
    content = XML::Parser.string(rmsg).parse
    pars = content.root.find('./parameter')
    return_value = nil
    pars.each do |p|
      if p.attributes.get_attribute("name").value == "status"
        return_value = p.content == "m3:Success" ? true : false
        break
      end 
    end

    # get the subscription id
    subscription_id = nil
    pars.each do |p|
      if p.attributes.get_attribute("name").value == "subscription_id"
        subscription_id = p.content
        break
      end 
    end
    puts subscription_id

    # store the subscription id and its socket
    @active_subscriptions[subscription_id] = {} 
    @active_subscriptions[subscription_id]["socket"] = tcpc

    # Get the initial results
    triple_list = []
    content = XML::Parser.string(rmsg).parse
    pars = content.root.find('./parameter')
    pars.each do |p|

      if p.attributes.get_attribute("name").value == "results"

        # new root
        nroot = p
        t = p.find('./triple_list').first.find('./triple')
        t.each do |tr|

          # get subject
          s = tr.find('./subject').first
          s_content = s.content.strip
          s_type = s.attributes.get_attribute("type").value.strip
          if s_type.downcase == "uri"
            subject = URI.new(s_content)
          else
            subject = Literal.new(s_content)
          end
    
          # get predicate
          p = tr.find('./predicate').first
          p_content = p.content.strip
          predicate = URI.new(p_content)
    
          # get object
          o = tr.find('./object').first
          o_content = o.content.strip
          o_type = o.attributes.get_attribute("type").value.strip
          if o_type.downcase == "uri"
            object = URI.new(o_content)
          else
            object = Literal.new(o_content)
          end

          # triple
          t = Triple.new(subject, predicate, object)
          triple_list << t        
    
        end
      end 
    end

    # start the indication receiver
    if myHandlerClass

      # TODO check if myHandlerClass is a Handler,
      # otherwise raise an exception
      h = myHandlerClass.new()

    else
      h = nil
    end
    t = Thread.new{rdf_indication_receiver(tcpc, subscription_id, h)}    
    @active_subscriptions[subscription_id]["thread"] = t

    # return
    return return_value, subscription_id, triple_list
    
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

    # storing last reply
    @last_reply = rmsg

    # increment transaction id
    @transaction_id += 1

    # parsing the message to get the return value
    content = XML::Parser.string(rmsg).parse
    pars = content.root.find('./parameter')
    return_value = nil
    pars.each do |p|
      if p.attributes.get_attribute("name").value == "status"
        return_value = p.content == "m3:Success" ? true : false
        break
      end 
    end

    # get the subscription id
    subscription_id = nil
    pars.each do |p|
      if p.attributes.get_attribute("name").value == "subscription_id"
        subscription_id = p.content
        break
      end 
    end
    puts subscription_id

    # store the subscription id and its socket
    @active_subscriptions[subscription_id] = {} 
    @active_subscriptions[subscription_id]["socket"] = tcpc

    # TODO: Get the initial results
    initial_results = []

    # start the indication receiver
    if myHandlerClass and myHandlerClass.is_a?(Handler)
      # TODO check if myHandlerClass is a Handler,
      # otherwise raise an exception
      h = myHandlerClass.new()
    else
      h = nil
    end
    t = Thread.new{sparql_indication_receiver(tcpc, subscription_id, h)}    
    @active_subscriptions[subscription_id]["thread"] = t

    # return
    return return_value, subscription_id, initial_results
    
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

      puts rmsg.green.bold

      # parse the message
      content = XML::Parser.string(rmsg).parse
      
      # is it an indication?
      if content.find('//message_type').first.content() == "INDICATION"

        # debug print
        if @debug
          @logger.debug("KP:rdf_indication_receiver -- INDICATION")
        end
        
        # extract triples from the indication and launch the handler
        puts "extracting triples"
        if handler and handler.is_a?(Handler)
          puts "i'm really extracting triples"
          added, removed = extract_rdf_triples_from_indication(content)
          h = handler.new()
          h.handle(added, removed)          
        end
        
      # it is an unsubscribe confirm
      else

        # close subscription
        pars = content.root.find('./parameter')
        pars.each do |p|
          if p.attributes.get_attribute("name").value == "status"
            if p.content == "m3:Success"

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
              t.exit()    

            end
          end 
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

      puts rmsg.green.bold

      # parse the message
      content = XML::Parser.string(rmsg).parse
      
      # is it an indication?
      if content.find('//message_type').first.content() == "INDICATION"

        # debug print
        if @debug
          @logger.debug("KP:sparql_indication_receiver -- INDICATION")
        end
        
        # extract triples from the indication and launch the handler
        puts "extracting triples"
        if handler and handler.is_a?(Handler)
          added, removed = extract_sparql_results_from_indication(content)
          h = handler.new()
          h.handle(added, removed)          
        end
        
      # it is an unsubscribe confirm
      else

        # close subscription
        pars = content.root.find('./parameter')
        pars.each do |p|
          if p.attributes.get_attribute("name").value == "status"
            if p.content == "m3:Success"

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
              t.exit()    

            end
          end 
        end

      end
      
    end
  end


  ####################################################
  #
  # RDF Subscription - Triples extraction
  #
  ####################################################

  def extract_rdf_triples_from_indication(content)

    # debug print
    if @debug
        @logger.debug("KP:extract_rdf_triples_from_indication")
    end

    # Get NEW and OLD triple list
    added = []
    removed = []
    pars = content.root.find('./parameter')
    pars.each do |p|

      # Extract added triples
      if p.attributes.get_attribute("name").value == "new_results"
        
        # new root
        nroot = p
        t = p.find('./triple_list').first.find('./triple')
        t.each do |tr|

          # get subject
          s = tr.find('./subject').first
          s_content = s.content.strip
          s_type = s.attributes.get_attribute("type").value.strip               
          if s_type.downcase == "uri"
            subject = URI.new(s_content)
          else
            subject = Literal.new(s_content)
          end

          # get predicate
          puts 'getting predicate'
          p = tr.find('./predicate').first
          p_content = p.content.strip
          predicate = URI.new(p_content)
          
          # get object
          o = tr.find('./object').first
          o_content = o.content.strip
          o_type = o.attributes.get_attribute("type").value.strip
          if o_type.downcase == "uri"
            object = URI.new(o_content)
          else
            object = Literal.new(o_content)
          end
          
          # build the triple
          added_triple = Triple.new(subject, predicate, object)
          added << added_triple

        end
        
      # Extract removed triples
      elsif p.attributes.get_attribute("name").value == "obsolete_results"

        # new root
        nroot = p
        t = p.find('./triple_list').first.find('./triple')
        t.each do |tr|
          
          # get subject
          s = tr.find('./subject').first
          s_content = s.content.strip
          s_type = s.attributes.get_attribute("type").value.strip               
          if s_type.downcase == "uri"
            subject = URI.new(s_content)
          else
            subject = Literal.new(s_content)
          end
          
          # get predicate
          p = tr.find('./predicate').first
          p_content = p.content.strip
          predicate = URI.new(p_content)
          
          # get object
          o = tr.find('./object').first
          o_content = o.content.strip
          o_type = o.attributes.get_attribute("type").value.strip
          if o_type.downcase == "uri"
            object = URI.new(o_content)
          else
            object = Literal.new(o_content)
          end
          
          # build the triple
          removed_triple = Triple.new(subject, predicate, object)
          removed << removed_triple
          
        end        
      end     
    end       
    
    # return added and removed triples
    return added, removed

  end


  ####################################################
  #
  # SPARQL Subscription - Results extraction
  #
  ####################################################

  def extract_sparql_results_from_indication(content)

    # debug print
    if @debug
        @logger.debug("KP:extract_sparql_results_from_indication")
    end

    # Get NEW and OLD triple list
    added = []
    removed = []

    # Get the result list
    content = XML::Parser.string(rmsg.strip).parse
    pars = content.root.find('./parameter')
    pars.each do |p|

      # Extract NEW results
      if p.attributes.get_attribute("name").value == "new_results"

        # we're on the sparql node
        p.each_element do |sparql|
    
          sparql.each_element do |hr|
          
            # head/results fields
            hr.each_element do |field|
                
              # find the results
              if field.name == "result"
    
                # We found a result
                result = []
                
                field.each_element do |n|
                  variable = []
                  variable << n.attributes.first.value
                  n.each_element do |v|
                    variable << v.name
                    variable << v.content
                  end
                  result << variable
                  added << result
                end

              end
            end        
          end
          break
        end
      end
    end

      ###
    puts added.size

    # return added and removed triples
    return added, removed

  end

end
