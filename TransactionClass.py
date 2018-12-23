"""
Class defined for a transaction object. Will contain an integer ID, a list of items, a date,
and the number of items in the transaction. 
"""

class Transaction:
    # Transaction attributes, constructor (?)
    def __init__(self, ID=0,items=[], date=''):
        self.ID = ID
        self.items = items
        self.date = date
        self.num_items = len(self.items)

    ######## SETTERS/GETTERS (or just getters because I'm lazy)
    # Method that returns the ID
    def getID(self):
        return self.ID
    
    # Function that returns the items in the transaction
    def getItems(self):
        return self.items

    # Function that returns the date of the transaction
    def getDate(self):
        return self.date

    # Function that returns the number of items in the transaction
    def getNumItems(self):
        return self.num_items

    ####### UTILITY FUNCTIONS
    # Function that adds an item to the transaction
    def addItem(self, item):
        self.items.append(item)
        self.num_items += 1

    # Function that returns the number of a certain item in a transaction
    def itemCount(self, item):
        counter = 0
        for i in range(0, self.num_items):
            if self.items[i] == item:
                counter += 1
        return counter

    # Function that prints a snippet of a transaction (similar to peek in R)
    def peek(self):
        if self.num_items >= 3:
            print("Transaction ID: {}    Date: {}   Total Items: {}    First items: {}, {}, {}..." \
                .format(self.ID, self.date, self.num_items, self.items[0], self.items[1], self.items[2]))
        else:
            print("Transaction ID: {}    Date: {}   Total Items:{}    Items: {}" \
                .format(self.ID, self.date, self.num_items, self.items))


if __name__ == "__main__":
    x = Transaction(11507009, ['apple', 'orange', 'bannana', 'other fruit'], '2018')
    y = Transaction()
    x.peek()
    y.peek()
    y.addItem('bananas')
    y.peek()

    