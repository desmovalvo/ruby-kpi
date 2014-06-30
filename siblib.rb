#!/usr/bin/ruby

# requirements
require "xml"
require "uuid"
require "socket"
require "rubygems"
require "nokogiri"
load "ssap_templates.rb"

# The URI class
class URI

  # accessor
  attr_reader :value

  # constructor
  def initialize(value)
    @value = value
  end

end


# The Literal class
class Literal

  # accessor
  attr_reader :value

  # constructor
  def initialize(value)
    @value = value
  end

end


# The Triple class
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


# The KP class
class KP

  # accessors
  attr_reader :last_request, :last_reply
  
  # constructor 
  def initialize(ip, port, smart_space)

    # instance variables
    @ip = ip
    @port = port
    @ss = smart_space
    @transaction_id = 1
    @node_id = UUID.new().generate() 
    @last_request = nil
    @last_reply = nil

  end


  # join
  def join_sib()

    # build the SSAP JOIN REQUEST
    msg = JOIN_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id ]

    # opening a socket to the SIB
    sib_socket = TCPSocket.new(@ip, @port)

    # sending the message
    sib_socket.write(msg)

    # waiting for a reply
    rmsg = sib_socket.recv(4096)

    # closing the socket
    sib_socket.close()

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
  

  # LEAVE
  def leave_sib()

    # build the SSAP JOIN REQUEST
    msg = LEAVE_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id ]

    # opening a socket to the SIB
    sib_socket = TCPSocket.new(@ip, @port)

    # sending the message
    sib_socket.write(msg)

    # waiting for a reply
    rmsg = sib_socket.recv(4096)

    # closing the socket
    sib_socket.close()

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


  # INSERT
  def insert(triple_list)

    # build the triple_string
    triple_string = ""
    triple_list.each do |triple|
      triple_string += TRIPLE_TEMPLATE % [triple.subject.class, triple.subject.value, triple.predicate.value, triple.object.class, triple.object.value]
    end

    # build the SSAP JOIN REQUEST
    msg = INSERT_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, triple_string ]

    # opening a socket to the SIB
    sib_socket = TCPSocket.new(@ip, @port)

    # sending the message
    sib_socket.write(msg)

    # waiting for a reply
    rmsg = sib_socket.recv(4096)

    # closing the socket
    sib_socket.close()

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

    # build the triple
    triple_string = ""
    triple_list.each do |triple|
      triple_string += TRIPLE_TEMPLATE % [triple.subject.class, triple.subject.value, triple.predicate.value, triple.object.class, triple.object.value]
    end

    # build the SSAP JOIN REQUEST
    msg = REMOVE_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, triple_string ]
  
    # opening a socket to the SIB
    sib_socket = TCPSocket.new(@ip, @port)

    # sending the message
    sib_socket.write(msg)

    # waiting for a reply
    rmsg = sib_socket.recv(4096)

    # closing the socket
    sib_socket.close()

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

  # QUERY
  def query(triple)

    # build the triple
    triple_string = TRIPLE_TEMPLATE % [triple.subject.class, triple.subject.value, triple.predicate.value, triple.object.class, triple.object.value]

    # build the SSAP JOIN REQUEST
    msg = QUERY_REQUEST_TEMPLATE % [ @node_id, @ss, @transaction_id, triple_string ]
  
    # opening a socket to the SIB
    sib_socket = TCPSocket.new(@ip, @port)

    # sending the message
    sib_socket.write(msg)

    # waiting for a reply
    rmsg = sib_socket.recv(4096)

    # closing the socket
    sib_socket.close()

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

end
