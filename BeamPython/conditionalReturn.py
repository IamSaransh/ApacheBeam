from apache_beam.io.textio import WriteToText
    import apache_beam as beam
    
    class FilterFn(beam.DoFn):
      """
      The rules I have assumed is to just perform a length check on the third column and 
      value check on forth column
      if(length of 3th column) >10 and (value of 4th column) is >500 then that record is good
      """
      def process(self, text):
    #------------------ -----isGoodRow BEGINS-------------------------------
        def isGoodRow(a_list):
          if( (len(a_list[2]) > 10) and (int(a_list[3]) >100) ):
            return True
          else:
            return False
    #------------------- ----isGoodRow ENDS-------------------------------
        a_list = []
        a_list = text.split(",") # this list contains all the column for a periticuar i/p Line
        bool_result = isGoodRow(a_list)
        if(bool_result == True):
          yield beam.TaggedOutput('good', text)
        else:
          yield beam.TaggedOutput('bad', text)
    
    with beam.Pipeline() as pipeline:
      split_me = (
          pipeline
          | 'Read from text file' >> beam.io.ReadFromText("/content/sample.txt")
          | 'Split words and Remove N/A' >> beam.ParDo(FilterFn()).with_outputs('good','bad')
          )
      good_collection = (
          split_me.good 
          |"write good o/p" >> beam.io.WriteToText(file_path_prefix='/content/good',file_name_suffix='.txt')
          ) 
      bad_collection = (
          split_me.bad 
          |"write bad o/p" >> beam.io.WriteToText(file_path_prefix='/content/bad',file_name_suffix='.txt')
          )