require 'rubygems'

if Gem::Specification::find_by_name('berkshelf').version.to_s[0] == '3'
  source 'https://api.berkshelf.com'
end

cookbook 'vertica', git: 'https://github.com/tkuhlman/cookbooks-vertica'

# community cookbooks
cookbook 'apt'
cookbook 'hostsfile', '= 1.0.1'
cookbook 'build-essential', '= 1.4.4'
cookbook 'sysctl', '= 0.4.0'
