import tempfile
import shutil
import os
from pyspark.sql.functions import col
from ingest_data import main


def test_csv_to_delta():
    # Create a temporary directory for the test
    tmp_dir = tempfile.mkdtemp()

    try:
        # Create a sample CSV file
        csv_file = os.path.join(tmp_dir, "test.csv")
        with open(csv_file, "w") as f:
            f.write("itemID,customerID,itemName,itemAmount,quantity\n")
            f.write("A01,CUS01,item1,100,1\n")
            f.write("A02,CUS02,item2,200,2\n")
            f.write("A03,CUS03,item3,300,3\n")

        # Set the input and output directories to the temporary directory
        os.environ["INPUT_DIR"] = tmp_dir
        os.environ["OUTPUT_DIR"] = tmp_dir

        # Run the main function
        main()

        # Read the output Delta Lake table
        output_df = spark.read.format("delta").load(os.path.join(tmp_dir, "delta_lake_dir"))

        # Verify the output DataFrame contains the expected data
        expected_df = spark.createDataFrame([
            ("A01", "CUS01", "item1", 100, 1),
            ("A02", "CUS02", "item2", 200, 2),
            ("A03", "CUS03", "item3", 300, 3),
        ], ["itemID", "customerID", "itemName", "itemAmount", "quantity"])
        expected_df.show()
        assert output_df.count() == 3
        assert output_df.select(col("itemID"), col("customerID"), col("itemName"), col("itemAmount"), col("quantity")).exceptAll(expected_df).count() == 0

    finally:
        # Clean up the temporary directory
        shutil.rmtree(tmp_dir)
