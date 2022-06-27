from mrjob.job import MRJob
from mrjob.step import MRStep

# The ratings class that contains the logic
class Ratings(MRJob):
  def steps(self):
    return [
      MRStep(
        mapper=self.getRatingCount,
        reducer=self.countRatings
      ),
      MRStep(
        reducer=self.sortRatings
      )
    ]

  # To achieve the grade 6
  # Get the amount of ratings for each movie
  def getRatingCount(self, _, line):
    # We parse the data, since we only really care about the movie Id we only define that variable.
    (_, movieId, _, _) = line.split('\t')

    # We return movieId and 1, where 1 is to indicate that there was a rating for that movie identifier, we use that 1 in the reducer to count the amount of ratings for a movie.
    yield movieId, 1

  # To achieve Grade 6
  # Reducer method that is used to count the amount of ratings a movie has.
  def countRatings(self, movieId, rating):
    yield None, (sum(rating), movieId)

  # To achieve Grade 8
  # Reducer method that is used to sort the movies based on the amount of ratings a movie has.
  def sortRatings(self, _, movies):
    # Sort the movies based on the ratings count, we set reverse so that it sorts from highest amount of ratings to lowest amount of ratings.
    for ratingCount, movieId in sorted(movies, reverse=True):
      yield(int(movieId), int(ratingCount))

if __name__ == '__main__':
  Ratings.run()
