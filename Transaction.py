"""
Class defined for a basket object. Will contain an integer ID, a list of items, a date,the number of items in the
basket, and the total price of the transaction
"""

class Basket:
    # Basket attributes, constructor (?)
    def __init__(self, ID=0, date='', items=[], cost=0):
        self.ID = ID
        self.items = items
        self.date = date
        self.num_items = len(self.items)
        self.cost = cost

    ######## SETTERS/GETTERS (or just getters because....why)
    # Method that returns the ID
    def getID(self):
        return self.ID
    
    # Function that returns the items in the Basket
    def getItems(self):
        return self.items

    # Function that returns the date of the Basket
    def getDate(self):
        return self.date

    # Function that returns the number of items in the Basket
    def getNumItems(self):
        return self.num_items

    # Function that returns the total cost of the basket (USD)
    def getCost(self):
        return self.cost

    ####### UTILITY METHODS
    # Function that adds an item to the Basket
    def addItem(self, item):
        self.items.append(item)
        self.num_items += 1

    # Function that returns the number of a certain item in a Basket
    def itemCount(self, item):
        counter = 0
        for i in range(0, self.num_items):
            if self.items[i] == item:
                counter += 1
        return counter

    # Function that prints a snippet of a Basket (similar to peek in R)
    def peek(self):
        if self.num_items >= 3:
            print("Basket ID: {}    Date: {}   Total Items: {}    First items: {}, {}, {}..." \
                .format(self.ID, self.date, self.num_items, self.items[0], self.items[1], self.items[2]))
        else:
            print("Basket ID: {}    Date: {}   Total Items:{}    Items: {}" \
                .format(self.ID, self.date, self.num_items, self.items))


# Main file, to make sure the object and methods function as desired  
if __name__ == "__main__":
    x = Basket(11207009, ['apple', 'orange', 'bannana', 'other fruit'], '2018')
    y = Basket()
    x.peek()
    y.peek()
    y.addItem('bananas')
    y.peek()

    