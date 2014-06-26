require 'rbbt-util'
require 'rbbt/workflow'

module DbNSFP
  extend Workflow

  input :mutations, :array, "Mutated Isoforms", nil
  task :annotate => :tsv do |mutations|
    database = DbNSFP.database
    database.unnamed = true
    dumper = TSV::Dumper.new :key_field => "Mutated Isoform", :fields => database.fields, :type => :list, :cast => :to_f
    dumper.init
    TSV.traverse mutations, :into => dumper, :bar => "DbNSFP", :type => :array do |mutation|
      p = database[mutation]
      next if p.nil?
      p.collect!{|v| v == -999 ? nil : v }
      [mutation, p]
    end
  end
  export_synchronous :dbNSFP
end

require 'rbbt/sources/db_NSFP'
