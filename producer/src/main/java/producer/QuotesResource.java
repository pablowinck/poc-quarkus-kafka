package producer;


import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import model.Quote;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.UUID;

@Path("/quotes")
public class QuotesResource {

    @Channel("quote-requests")
    Emitter<String> quoteRequestEmitter;

    @Channel("quotes")
    Multi<Quote> quotes;

    @POST
    @Path("/request")
    @Produces(MediaType.TEXT_PLAIN)
    public String createRequest() {
        UUID uuid = UUID.randomUUID();
        Log.info("Creating quote request " + uuid);
        quoteRequestEmitter.send(uuid.toString());
        return uuid.toString();
    }


    @GET
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public Multi<Quote> stream() {
        Log.info("Streaming quotes");
        quotes.subscribe().with(
                quote -> Log.info("Received quote: " + quote),
                failure -> Log.error("Failed to process quote", failure),
                () -> Log.info("Completed processing quotes")
        );
        return quotes.log();
    }
}
