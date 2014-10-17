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

  # get_subscription_id
  def get_subscription_id()

    # parse the parameters section
    subscription_id = nil
    pars = @content.root.find('./parameter')
    pars.each do |p|
      if p.attributes.get_attribute("name").value == "subscription_id"
        subscription_id = p.content
        break
      end 
    end
    
    # return the subscription_id
    return  subscription_id

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

    mt = get_transaction_type()

    # It is a query?
    if  mt == "QUERY" or mt == "SUBSCRIBE"

      # Get the triple list
      triple_list = []
      pars = @content.root.find('./parameter')
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
      
      return triple_list

    else
      
      # do nothing
      # TODO: raise an exception

    end
  end

  # get_sparql_results
  def get_sparql_results()

    mt = get_transaction_type()
    if mt == "QUERY" or mt == "SUBSCRIBE"

      # declare an empty list
      results = []

      # Get the triple list
      pars = @content.root.find('./parameter')
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
      
      # return
      return results

    else
      
      # do nothing

    end

  end

  # get_rdf_triples_from_indication
  def get_rdf_triples_from_indication()

    # Get NEW and OLD triple list
    added = []
    removed = []
    pars = @content.root.find('./parameter')
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

  # get_sparql_results_from_indication
  def get_sparql_results_from_indication()

    puts "method started"

    # declare an empty list
    added = []
    removed = []

    # Get the triple list
    pars = @content.root.find('./parameter')
    pars.each do |p|
      
      puts p.attributes.get_attribute("name").value

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
        
      elsif p.attributes.get_attribute("name").value == "obsolete_results"

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
                  removed << result
                end
                
              end
            end        
          end
          break
        end
        
      end
    end

    return added, removed

  end

end
