import java.io.Serializable;

/**
 * Created by hossein on 11/30/15.
 */
public class ShTweet implements Serializable,Comparable {


    private Long date;
    private Integer follower;
    private Integer following;
    private Integer tweets;

    public ShTweet(Long date, Integer follower, Integer following, Integer tweets){
        this.date = date;
        this.follower = follower;
        this.following = following;
        this.tweets = tweets;
    }

    public Long getDate() {
        return date;
    }

    public void setDate(Long date) {
        this.date = date;
    }

    public Integer getFollower() {
        return follower;
    }

    public void setFollower(Integer follower) {
        this.follower = follower;
    }

    public Integer getFollowing() {
        return following;
    }

    public void setFollowing(Integer following) {
        this.following = following;
    }

    public Integer getTweets() {
        return tweets;
    }

    public void setTweets(Integer tweets) {
        this.tweets = tweets;
    }


    public int compareTo(Object otherStudent) {
        if (otherStudent instanceof ShTweet) {
            ShTweet u = (ShTweet)otherStudent;
            if (this.getDate() > u.getDate())
                return 1;
            else
                return -1;
        }
        return 0;

    }

    public String toString(){
        return String.valueOf(this.date)+ "," + String.valueOf(this.follower)+ "," + String.valueOf(this.tweets);//+ "," + String.valueOf(this.date)+ ",";
    }
}
