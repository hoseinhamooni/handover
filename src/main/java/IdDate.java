import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by hossein on 10/2/15.
 */
public class IdDate implements Serializable,Comparable{
    private String id;
    private Long date;
    private Integer follower;
    private Integer tweets;

    public IdDate(String name , Long date){
        this.id = name;
        this.date = date;
    }

    public IdDate(String name , Long date, Integer follower, Integer tweets){
        this.id = name;
        this.date = date;
        this.follower = follower;
        this.tweets = tweets;
    }

    public void setDate(Long date) {
        this.date = date;
    }

    public Long getDate() {
        return date;
    }

    public void setId(String name) {
        this.id = name;
    }

    public String getId() {
        return id;
    }



    @Override public boolean equals(Object other) {
        boolean result = false;
        if (other instanceof IdDate) {
            IdDate that = (IdDate) other;
            result = (this.getId().equals(that.getId()));
        }
        return result;
    }

    @Override public int hashCode() {
        return (41 * (41 + getId().length()));
    }



    public int compareTo(Object otherStudent) {
        if (otherStudent instanceof IdDate) {
            IdDate u = (IdDate) otherStudent;
//            if (this.getId().equals(u.getId())) {
//                return 0;
//            }
            if (this.getDate() > u.getDate())
                return 1;
            else
                return -1;

        }
        return 0;
    }

    @Override
    public String toString() {
        return this.getId()+","+this.getDate()+","+this.getFollower()+","+this.getTweets();
    }


    public Integer getTweets() {
        return tweets;
    }

    public void setTweets(Integer tweets) {
        this.tweets = tweets;
    }

    public Integer getFollower() {
        return follower;
    }

    public void setFollower(Integer follower) {
        this.follower = follower;
    }
}
