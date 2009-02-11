# This is a placeholder for now.  Optionally allow us to create a
# filesystem structure based on:
#
# /storageDir/hostname/dbName/tableName.sql.gz
# /storageDir/hostname/dbName/user.yml
#
# This will facilitate a clean backup storage
module Mysqladmin
  class FileSystem
  end
end