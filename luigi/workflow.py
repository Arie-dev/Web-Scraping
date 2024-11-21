import os
import luigi
# Ensure output directory exists
if not os.path.exists('output'):
    os.makedirs('output')



# Import the required libraries
from Step1_Books_Bronze import booksBronzeLayer
from Step1_Quotes_Bronze import quotesBronzeLayer
from Step2_Cleaning_Silver import process_silver_layer
from Step3_Enrich_Gold import process_gold_layer



# Task 1: booksBronzeLayer


class BooksBronzeLayer(luigi.Task):
    def output(self):
        # Define where the output of this task is saved
        return luigi.LocalTarget('output/books_bronze_layer.txt')

    def run(self):
        # Call your actual function
        booksBronzeLayer()
        # Write a "done" marker or any output for tracking
        with self.output().open('w') as f:
            f.write("Books Bronze Layer completed")

# Task 1: quotesBronzeLayer


class QuotesBronzeLayer(luigi.Task):
    def output(self):
        return luigi.LocalTarget('output/quotes_bronze_layer.txt')

    def run(self):
        quotesBronzeLayer()
        with self.output().open('w') as f:
            f.write("Quotes Bronze Layer completed")

# Task 2: processSilverLayer


class ProcessSilverLayer(luigi.Task):
    def requires(self):
        # Depend on both bronze layer tasks
        return [BooksBronzeLayer(), QuotesBronzeLayer()]

    def output(self):
        return luigi.LocalTarget('output/silver_layer.txt')

    def run(self):
        process_silver_layer()
        with self.output().open('w') as f:
            f.write("Silver Layer completed")

# Task 3: processGoldLayer


class ProcessGoldLayer(luigi.Task):
    def requires(self):
        # Depend on the silver layer task
        return ProcessSilverLayer()

    def output(self):
        return luigi.LocalTarget('output/gold_layer.txt')

    def run(self):
        process_gold_layer()
        with self.output().open('w') as f:
            f.write("Gold Layer completed")


# Running the Luigi pipeline
if __name__ == '__main__':
    luigi.run()
