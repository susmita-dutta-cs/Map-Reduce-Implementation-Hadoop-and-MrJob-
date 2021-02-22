



from mrjob.job import MRJob
from mrjob.step import MRStep


class TopProduct(MRJob):
    # pre-processing
    def mapper(self, _, line):
        # Removes the first line in csv
        try:
            split_line = line.split(',')
            # The description already have commas.
            # So the fields need to be extracted from the right
            length = len(split_line)
            stock_code = split_line[1]
            quantity = float(split_line[length - 5])
            revenue = float(split_line[length - 3]) * float(split_line[ length - 5])

            yield stock_code , (revenue, quantity)

        except:
            yield None ,0

    def combiner(self, stock_code, combined_all):
        if stock_code != None:
            quan  = 0
            rev = 0
            for q, r in combined_all:
                quan += q
                rev += r
            yield stock_code, (quan, rev)

    def reducer(self, stock_code, combined_all):
        if stock_code != None:
            quan = 0
            rev = 0
            for q, r in combined_all:
                quan += q
                rev += r
            yield None, (quan, rev, stock_code)

    def reducer_final_product(self, _, q_r_stock_code):
        quan = 0
        rev = 0
        prod_q = ''
        prod_rev = ''

        for a, b, c, in q_r_stock_code:
            if a > quan:
                quan = a
                prod_q = c

            if b > rev:
                rev = b
                prod_r = c
        yield prod_q, prod_r

    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer),
            MRStep(reducer = self.reducer_final_product)
        ]

if __name__ == '__main__':
    TopProduct.run()