import java.io.Serializable;
import java.util.Objects;

public class MovieKey implements Serializable {
    String movieName;
    String movieId;

    public MovieKey(String movieId, String movieName){
        this.movieId = movieId;
        this.movieName = movieName;
    }

    public MovieKey(MovieKey mk){
        this.movieName = mk.movieName;
        this.movieId = mk.movieId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieKey movieKey = (MovieKey) o;
        return Objects.equals(movieId, movieKey.movieId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movieId);
    }
}
