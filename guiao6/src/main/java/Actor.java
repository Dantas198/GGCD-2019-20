import java.io.Serializable;

public class Actor implements Comparable<Actor>, Serializable {
    ActorKey actorKey;
    int numMovies;

    public Actor(ActorKey actorKey, int numMovies){
        this.actorKey = actorKey;
        this.numMovies = numMovies;
    }

    @Override
    public int compareTo(Actor o) {
        return this.numMovies - o.numMovies;
    }
}
