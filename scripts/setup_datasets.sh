# Define searched schema.org class
array=( Movie )

for i in "${array[@]}"
do
	# Create folder
	mkdir -p ./data/corpus/${i,,}
	cd ./data/corpus/${i,,}

	# Retrieve data sets
        wget http://data.dws.informatik.uni-mannheim.de/structureddata/schemaorgtables/$i/full/$i\_top100.zip
        unzip $i\_top100.zip
        mv $i\_top100.zip ../$i\_top100.zip

	wget http://data.dws.informatik.uni-mannheim.de/structureddata/schemaorgtables/$i/full/$i\_minimum3.zip
	unzip $i\_minimum3.zip
        mv $i\_minimum3.zip ../$i\_minimum3.zip

        wget http://data.dws.informatik.uni-mannheim.de/structureddata/schemaorgtables/$i/full/$i\_rest.zip
        unzip $i\_rest.zip
        mv $i\_rest.zip ../$i\_rest.zip

	cd ../../../
done
