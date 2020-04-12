import org.apache.spark.api.java.Optional;

import java.io.Serializable;

public class MovieInfo implements Serializable {
    String title;
    float rating;

    public MovieInfo(String title, Optional<Float> rating) {
        this.title = title;
        if(rating.isPresent())
            this.rating = rating.get();
        else
            this.rating = 0;
    }

    public float getRating() {
        return this.rating;
    }
}
