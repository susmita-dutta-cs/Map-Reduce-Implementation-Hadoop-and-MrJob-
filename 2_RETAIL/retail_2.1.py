#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 12 23:16:19 2020

@author: susmita
"""


from mrjob.job import MRJob
from mrjob.step import MRStep


class TopCustomer(MRJob):
    # pre-processing
    def mapper(self, _, line):
        # Removes the first line in csv
        try:
            split_line = line.split(',')
            length = len(split_line)
            customer_id = split_line[length - 2]
            revenue = float(split_line[length - 3]) * float(split_line[ length - 5])
            date = split_line[length - 4].split(" ")
            # Last two digits of the year field.
            year = int(date[0][-2] + date[0][-1])
            yield ((customer_id , year) , revenue)

        except:
            yield (None ,0)

    def combiner(self, comb_id , revenue):
        yield comb_id , sum(revenue)

    def reducer(self , comb_id , revenue):
        if comb_id != None:
            id , year = comb_id
            total_rev = sum(revenue)
            yield year , (total_rev, id)

    def reducer1(self, year, rev_id):
        max_rev = 0
        max_cust = ''
        for rev, id in rev_id:
            if rev > max_rev:
                max_rev = rev
                max_cust = id
        yield year , (max_cust, max_rev)

    def steps(self):
        return [
            MRStep(mapper = self.mapper, combiner = self.combiner, reducer = self.reducer) ,
            MRStep(reducer = self.reducer1)
        ]

if __name__ == '__main__':
    TopCustomer.run()