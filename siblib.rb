#!/usr/bin/ruby

# requirements
require "xml"
require "uuid"
require "socket"
load "TCPConnection.rb"
load "ssap_templates.rb"


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
  
end


##################################################
#
# The KP class
#
##################################################

class KP

  # accessors
  attr_reader :last_request, :last_reply
  
  # constructor 
  def initialize(ip, port, smart_space, debug = false)

    # instance variables
    @ip = ip
    @port = port
    @debug = debug
    @ss = smart_space
    @transaction_id = 1
    @node_id = UUID.new().generate() 
    @last_request = nil
    @last_reply = nil
    @active_subscriptions = {}

  end


  # join
  def join_sib()

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
  

  # LEAVE
  def leave_sib()

    # building and storing the SSAP LEAVE REQUEST
    msg = LEAVE_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id ]
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


  # INSERT
  def insert(triple_list)

    # is triple_list an Array or a Triple?
    if triple_list.class.to_s == "Triple"
      triple_list = [triple_list]
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


  # REMOVE
  def remove(triple_list)

    # is triple_list an Array or a Triple?
    if triple_list.class.to_s == "Triple"
      triple_list = [triple_list]
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

  # QUERY
  def query(triple)

    # build the triple
    triple_string = TRIPLE_TEMPLATE % [triple.subject.class, triple.subject.value, triple.predicate.value, triple.object.class, triple.object.value]

    # build and store the SSAP QUERY REQUEST
    msg = QUERY_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, triple_string ]
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


  # SPARQL QUERY
  def sparql_query(q)

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

  # RDF Subscription
  def subscribe(triple)

    # build the triple
    triple_string = TRIPLE_TEMPLATE % [triple.subject.class, triple.subject.value, triple.predicate.value, triple.object.class, triple.object.value]

    # build and store the SSAP QUERY REQUEST
    msg = SUBSCRIBE_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, triple_string ]
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
    @active_subscriptions[subscription_id] = tcpc

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
    
    return return_value, triple_list
    
  end


  # unsubscribe
  def unsubscribe(sub_id)

    # building and storing the SSAP UNSUBSCRIBE REQUEST
    msg = UNSUBSCRIBE_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, sub_id ]
    @last_request = msg

    # connecting to the SIB
    tcpc = TCPConnection.new(@ip, @port)
    
    # sending the request  message
    tcpc.send_request(msg)

    # closing the socket
    tcpc.close()

    # waiting for a reply on the socket used for the subscribe request
    rmsg = @active_subscriptions[sub_id].receive_reply()
    @active_subscriptions[sub_id].close()
    @active_subscriptions.delete(sub_id)

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

end
