import java.io.Serializable;
import java.util.Objects;

public class ActorKey implements Serializable {
    String actorName;
    String actorId;

    public ActorKey(String actorName, String actorId){
        this.actorName = actorName;
        this.actorId = actorId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActorKey actorKey = (ActorKey) o;
        return Objects.equals(actorId, actorKey.actorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actorId);
    }

    @Override
    public String toString() {
        return "(" + actorName + "," + actorId + ")";
    }
}