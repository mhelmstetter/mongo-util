package com.mongodb.util.spring;

import java.text.ParseException;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.ExchangeFunction;

import me.vzhilin.auth.DigestAuthenticator;
import me.vzhilin.auth.parser.ChallengeResponse;
import me.vzhilin.auth.parser.ChallengeResponseParser;
import reactor.core.publisher.Mono;

public class DigestAuthFilterFunction implements ExchangeFilterFunction {

    private final DigestAuthenticator digestAuthenticator;

    public DigestAuthFilterFunction(DigestAuthenticator digestAuthenticator) {
        this.digestAuthenticator = digestAuthenticator;
    }

    @Override
    public Mono<ClientResponse> filter(ClientRequest request, ExchangeFunction next) {
        return next.exchange(addAuthorizationHeader(request))
                .flatMap(response -> {
                    if (response.rawStatusCode() == 401) {
                        updateAuthenticator(response.headers().asHttpHeaders());
                        return next.exchange(addAuthorizationHeader(request))
                                .doOnNext(res -> {
                                    if (res.rawStatusCode() == 401) {
                                        updateAuthenticator(res.headers().asHttpHeaders());
                                    }
                                });
                    } else {
                        return Mono.just(response);
                    }
                });
    }

    private ClientRequest addAuthorizationHeader(ClientRequest request) {
        String authorization = digestAuthenticator.authorizationHeader(request.method().name(), request.url().getPath());
        if (authorization != null) {
            return ClientRequest.from(request)
                    .headers(headers -> headers.set(HttpHeaders.AUTHORIZATION, authorization))
                    .build();
        } else {
            return request;
        }
    }

    private void updateAuthenticator(HttpHeaders headers) {
        String wwwAuthenticateHeader = headers.getFirst(HttpHeaders.WWW_AUTHENTICATE);
        if (wwwAuthenticateHeader != null) {
            try {
                ChallengeResponse challenge = new ChallengeResponseParser(wwwAuthenticateHeader).parseChallenge();
                digestAuthenticator.onResponseReceived(challenge, 401);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Consumer<List<ExchangeFilterFunction>> setDigestAuth(DigestAuthenticator digestAuthenticator) {
        return filters -> filters.add(0, new DigestAuthFilterFunction(digestAuthenticator));
    }
}