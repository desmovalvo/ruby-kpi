##################################################
#
# The TCPConnection class
#
##################################################

class TCPConnection

  # Constructor
  def initialize(ip, port)

    # Read parameters
    @ip = ip
    @port = port

    # Connect
    begin
      @sib_socket = TCPSocket.new(@ip, @port)
    rescue Errno::ECONNREFUSED
      raise SIBError, 'Connection refused'
    end
  end

  # Send the request message
  def send_request(msg)  
    @sib_socket.write(msg)
  end

  # Receive the reply
  def receive_reply()
    rmsg = ""
    while true do
      begin
        r = @sib_socket.recv(4096)
        rmsg += r
        if rmsg.include?("</SSAP_message>")
          break
        end
      rescue
        raise SIBError, 'Error while receiving a reply'
      end
    end

    return rmsg
  end

  # Close the connection
  def close()
    @sib_socket.close()
  end

end
