import os
import luigi
if not os.path.exists('output'):
    os.makedirs('output')

from Step1_Books_Bronze import booksBronzeLayer
from Step1_Quotes_Bronze import quotesBronzeLayer
from Step2_Cleaning_Silver import process_silver_layer
from Step3_Enrich_Gold import process_gold_layer

class BooksBronzeLayer(luigi.Task):
    def output(self):
        return luigi.LocalTarget('output/books_bronze_layer.txt')

    def run(self):
        booksBronzeLayer()
        with self.output().open('w') as f:
            f.write("Books Bronze Layer completed")

class QuotesBronzeLayer(luigi.Task):
    def output(self):
        return luigi.LocalTarget('output/quotes_bronze_layer.txt')

    def run(self):
        quotesBronzeLayer()
        with self.output().open('w') as f:
            f.write("Quotes Bronze Layer completed")

class ProcessSilverLayer(luigi.Task):
    def requires(self):
        return [BooksBronzeLayer(), QuotesBronzeLayer()]

    def output(self):
        return luigi.LocalTarget('output/silver_layer.txt')

    def run(self):
        process_silver_layer()
        with self.output().open('w') as f:
            f.write("Silver Layer completed")

class ProcessGoldLayer(luigi.Task):
    def requires(self):
        return ProcessSilverLayer()

    def output(self):
        return luigi.LocalTarget('output/gold_layer.txt')

    def run(self):
        process_gold_layer()
        with self.output().open('w') as f:
            f.write("Gold Layer completed")


if __name__ == '__main__':
    luigi.run()
