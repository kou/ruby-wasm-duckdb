#!/usr/bin/env ruby

require "arrow"

table = Arrow::Table.new(int: [1, 2, 3],
                         string: ["a", "b", "c"])
table.save("data.arrows")
