# Simple script for validating wheels built for a Python release.
#
# Requires rubyzip (https://github.com/rubyzip/rubyzip)

require 'optparse'
require 'set'
require 'zip'

args = {}

parser = OptionParser.new
parser.on('-z', "--zipfile ZIPFILE", "Downloaded ZIP file") do |z|
  args[:zipfile] = z
end
parser.on('-v', "--version VERSION", "Expected version") do |v|
  args[:version] = v
end
parser.parse!

# This regex is largely derived from experimentation with built wheels coming off of cibuildwheel
# as well as info from PEP 425 and the binary distrubtion format spec
# (https://packaging.python.org/en/latest/specifications/binary-distribution-format/)
z1_name_regex = /^cassandra_driver-#{args[:version]}-cp(?<python_tag>\d+)-cp(?<abi_tag>\d+)-.*$/
expected_python_versions = Set["310", "311", "312", "313", "314"]
pversions = {}
pversions.default = 0

native_regex = /^cassandra\/(?<native_name>(io\/)?\w+?)\.cp.+?\.(so|pyd)$/

expected_shared_objects = %w[bytesio cluster cmurmur3 concurrent connection cqltypes cython_marshal
cython_utils deserializers ioutils metadata numpy_parser obj_parser parsing pool protocol query
row_parser util io/libevwrapper]
expected_native_names = Set.new(expected_shared_objects)

# Extract individual wheel files from the archive built by the Github upload-artifact task.
# Wheel files are ZIP files so we extract each entry and perform checks on them as well.
Zip::File::open(args[:zipfile]) do |z1|
  z1.each do |z1_entry|
    puts "z1_path: #{z1_entry.name}"
    m1 = z1_name_regex.match z1_entry.name

    # A pair of very basic sanity checks.  First, does the wheel filename match our regex above?  And second: do
    # the Python and ABI tags match?  For wheels that aren't pure Python the same syntax should be used for both
    # tags and they should match up.  See PEP 425 for more on this point.
    puts m1 ? "Wheel name matches" : "WARNING: Wheel name doesn't match"
    puts m1[:python_tag] == m1[:abi_tag] ? "Python and ABI tags match" : "WARNING: Python and ABI tags do not match"
    pversion = m1[:python_tag]
    pversions[pversion] = pversions[pversion] + 1

    # Extract names of all the native shared objects from the wheel and make sure that the set exactly matches the
    # expected set.  We want to compare this both ways; we should have everything we expect _and_ there shouldn't be
    # any other unexpected shared objects in the wheel.
    #
    # TODO: If and when we get to putting this into an automated build probably should put all of this in a
    # begin/rescue block with the File.delete op below in an ensure block.
    z1_entry.extract(destination_directory: "/tmp")
    z2_path = "/tmp/#{z1_entry.name}"
    Zip::File::open(z2_path) do |z2|
      native_names = z2.map { |x| x.name.strip }
        .filter { |x| x.end_with? ".so" or x.end_with? ".pyd" }
        .map do |x|
          m2 = native_regex.match x
          m2[:native_name]
        end
      observed_native_names = Set.new(native_names)
      diff1 = expected_native_names - observed_native_names
      puts diff1.empty? ? "All expected native names observed" : "WARNING: Not all native names observed #{diff1}"
      diff2 = observed_native_names - expected_native_names
      puts diff2.empty? ? "No extra native names observed" : "WARNING: Extra native names observed #{diff2}"
    end
    File.delete z2_path
  end

  # Next check to make sure that we saw wheels for each supported Python version.  As above we should have wheels
  # for all the supported Python versions and no others.
  observed_python_versions = Set.new(pversions.keys)
  diff1 = expected_python_versions - observed_python_versions
  puts diff1.empty? ? "All expected Python versions observed" : "WARNING: Not all Python versions observed #{diff1}"
  diff2 = observed_python_versions - expected_python_versions
  puts diff2.empty? ? "No extra Python versions observed" : "WARNING: Extra Python versions observed #{diff2}"

  # Finally check to make sure we have the same number of wheels for each Python version.  In some cases we might build
  # multiple wheels for a given Python version; this is true on Linux, for instance, where we build wheels for
  # x86_64 and arm64.
  puts Set.new(pversions.values).length == 1 ? "Only one size observed in versions set" : "WARNING: Multiple sizes observed in versions set"
end
