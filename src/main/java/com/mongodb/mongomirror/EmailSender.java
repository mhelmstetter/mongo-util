package com.mongodb.mongomirror;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.*;

public class EmailSender implements MongoMirrorEventListener {
    private Properties props = System.getProperties();
    /* When we first see an error, keep collecting errors for this long */
    private int errorMsgWindowSecs;
    /* How many errors to include in the email report. Keep fairly low to avoid memory pressure */
    private int errorRptMax;
    /* Max number of emails to send per run */
    private int totalEmailsMax;
    private List<String> recipients;
    private int emailsSent;
    private List<String> errors;
    private boolean inErrMsgWindow;
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());


    public EmailSender(List<String> recipients, int errorMsgWindowSecs, int errorRptMax, int totalEmailsMax) {
        logger.info("Initializing email sender");
        this.recipients = recipients;
        this.errorMsgWindowSecs = errorMsgWindowSecs;
        this.errorRptMax = errorRptMax;
        this.totalEmailsMax = totalEmailsMax;

        emailsSent = 0;
        errors = new ArrayList<>();
        inErrMsgWindow = false;

        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.auth", "true");
        props.put("mail.from", "matt.holford@mongodb.com");
        props.put("mail.from.password", "eyglhyjwcaaqwper");
    }

    public void sendReport(boolean success, List<String> errors, List<String> recipients) throws MessagingException {
        logger.info(String.format("Sending an email report with %s errors", errors.size()));
        Session session = Session.getInstance(props, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(props.getProperty("mail.from"),
                        props.getProperty("mail.from.password"));
            }
        });

        MimeMessage msg = composeMessage(success, errors, session);
        msg.setFrom(new InternetAddress(props.getProperty("mail.from")));
        for (String r : recipients) {
            msg.addRecipient(Message.RecipientType.TO, new InternetAddress(r));
        }

        Transport.send(msg);
    }

    private MimeMessage composeMessage(boolean success, List<String> errors, Session session) throws MessagingException {
        String not = success ? "" : "not ";
        String subj = String.format("MongoMirror execution was %s successful", not);
        StringBuilder body = new StringBuilder();
        body.append(String.format("There were %d errors reported", errors.size()));
        body.append("\n\n");
        if (errors.size() > 0) {
            for (String e : errors) {
                body.append(e);
                body.append("\n\n");
            }
        }
        body.append("Sincerely,\n\n");
        body.append("MongoDB PS");

        MimeMessage msg = new MimeMessage(session);
        msg.setSubject(subj);
        msg.setText(body.toString());
        return msg;
    }

    private void addError(String err) {
        /* Ignore error messages after we've hit the max number for this report */
        if (errors.size() < errorRptMax) {
            errors.add(err);

            if (!inErrMsgWindow) {
                /* It's either the first error we've seen or the first in a while */
                logger.info("Opening an error message window");
                inErrMsgWindow = true;
                Timer timer = new Timer("Error window timer");
                long delay = errorMsgWindowSecs * 1000;
                TimerTask t = new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            /* When the "window" timer elapses, send a report */
                            logger.info("Closing error message window");
                            sendReport(false, errors, recipients);
                            emailsSent++;
                            errors.clear();
                            inErrMsgWindow = false;
                        } catch (MessagingException e) {
                            logger.error("Exception thrown while sending email report", e);
                        }
                    }
                };
                timer.schedule(t, delay);
            }
        }
    }

    @Override
    public void procLoggedError(String msg) {
        logger.info("Trapped an error message");
        if (emailsSent < totalEmailsMax) {
            /* If we've already sent out the max reports for this MM run, do nothing */
            addError(msg);
        }
    }

    @Override
    public void procLoggedComplete(String msg) {
        logger.info("Trapped process completion");
        /* Send a final email report, unless we're in an error "window" */
        /* in which case sufficient detail will be in that report which is still being generated */
        if (!inErrMsgWindow) {
            try {
                sendReport(errors.size() > 0, errors, recipients);
            } catch (MessagingException e) {
                logger.error("Exception thrown while sending email report", e);
            }
        }
    }
}
