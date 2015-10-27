import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by hossein on 10/2/15.
 */
public class URLdate implements Serializable,Comparable{
    private String name;
    private Long date;
    private Integer follower;
    private Integer tweets;

    public Integer getFollower() {
        return follower;
    }

    public void setFollower(Integer follower) {
        this.follower = follower;
    }

    public Integer getTweets() {
        return tweets;
    }

    public void setTweets(Integer tweets) {
        this.tweets = tweets;
    }



    public URLdate(String name , Long date, Integer follower, Integer tweets){
        this.name = name;
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

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

//    public int compare(Object o, Object t1) {
//        if (o instanceof URLdate && t1 instanceof URLdate) {
//            URLdate u1 = (URLdate)o;
//            URLdate u2 = (URLdate)t1;
//            if (u1.getDate() > u2.getDate())
//                return 1;
//            if (u1.getDate() < u2.getDate())
//                return -1;
//            return 0;
//        }
//        return 0;
//    }

    @Override public boolean equals(Object other) {
        boolean result = false;
        if (other instanceof URLdate) {
            URLdate that = (URLdate) other;
            result = (this.getName().equals(that.getName()));
        }
        return result;
    }

    @Override public int hashCode() {
        return (41 * (41 + getName().length()));
    }

    public int compareTo(Object otherStudent) {
        if (otherStudent instanceof URLdate) {
            URLdate u = (URLdate)otherStudent;
//            if (this.getName().equals(u.getName()))
//                return 0;
            if (this.getDate() > u.getDate())
                return 1;
            else
                return -1;
        }
        return 0;

    }

    @Override
    public String toString() {
        return this.getName()+","+this.getDate()+","+this.getFollower()+","+this.getTweets();
    }


}
