class ReverseSort:
    def __init__(self, row):
        self.row = row

    def __eq__(self, other):
        return self.row == other.row

    # Note: this is intentionally backwards!
    def __lt__(self, other):
        return self.row > other.row
