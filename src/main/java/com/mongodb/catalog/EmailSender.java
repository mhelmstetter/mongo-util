package com.mongodb.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.Collection;

import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;

public class EmailSender {

	private static Logger logger = LoggerFactory.getLogger(EmailSender.class);

	private Properties props = System.getProperties();

	private String smtpHost;
	private int smtpPort;
	private boolean smtpTls;
	private boolean smtpAuth;
	private String smtpPassword;
	private String emailFrom;
	private InternetAddress emailFromAddress;

	private List<String> emailRecipients = new ArrayList<>();

	public void init() throws AddressException {
		props.put("mail.smtp.host", smtpHost);
		props.put("mail.smtp.port", smtpPort);
		props.put("mail.smtp.starttls.enable", smtpTls);
		props.put("mail.smtp.auth", smtpAuth);

		// TODO check that these are configured and throw a better error than NPE
		if (emailFrom == null) {
			
		}
		emailFromAddress = new InternetAddress(emailFrom);
		props.put("mail.from", emailFrom);
		props.put("mail.from.password", smtpPassword);
	}

	public void sendReport(String name, Set<Collection> newCollections, Set<Collection> droppedCollections)
			throws MessagingException {

		Session session = Session.getInstance(props, new Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(props.getProperty("mail.from"),
						props.getProperty("mail.from.password"));
			}
		});

		MimeMessage msg = composeMessage(name, newCollections, droppedCollections, session);
		msg.setFrom(emailFromAddress);
		for (String r : emailRecipients) {
			msg.addRecipient(Message.RecipientType.TO, new InternetAddress(r));
		}

		Transport.send(msg);
	}
	
	public void sendUuidFailureReport(String name, List<String> uuidFailures)
			throws MessagingException {

		Session session = Session.getInstance(props, new Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(props.getProperty("mail.from"),
						props.getProperty("mail.from.password"));
			}
		});

		MimeMessage msg = composeUuidFailureMessage(name, uuidFailures, session);
		msg.setFrom(emailFromAddress);
		for (String r : emailRecipients) {
			msg.addRecipient(Message.RecipientType.TO, new InternetAddress(r));
		}

		Transport.send(msg);
	}

	private MimeMessage composeMessage(String name, Set<Collection> newCollections, Set<Collection> droppedCollections, Session session)
			throws MessagingException {
		String subj = String.format("%s: Schema Change Detected", name);
		

		StringBuilder body = new StringBuilder();
		body.append(String.format("Found %s new collections: ", newCollections.size()));
		body.append("\n");
		
		for (Collection c : newCollections) {
			body.append("    ");
			body.append(c.getNamespace().getNamespace());
			body.append("\n");
		}
		body.append("\n");
		body.append(String.format("Found %s dropped collections: ", droppedCollections.size()));
		body.append("\n");
		
		for (Collection c : droppedCollections) {
			body.append("    ");
			body.append(c.getNamespace().getNamespace());
			body.append("\n");
		}

		MimeMessage msg = new MimeMessage(session);
		msg.setSubject(subj);
		msg.setText(body.toString());
		return msg;
	}
	
	private MimeMessage composeUuidFailureMessage(String name, List<String> uuidFailures, Session session)
			throws MessagingException {
		String subj = String.format("%s: UUID mismatch detected", name);
		

		StringBuilder body = new StringBuilder();
		body.append(String.format("Found %s uuid mismatches: ", uuidFailures.size()));
		body.append("\n");
		
		for (String failure : uuidFailures) {
			body.append("  ");
			body.append(failure);
			body.append("\n");
		}

		MimeMessage msg = new MimeMessage(session);
		msg.setSubject(subj);
		msg.setText(body.toString());
		return msg;
	}

	public void addEmailRecipient(String address) {
		emailRecipients.add(address);
	}

	public String getSmtpHost() {
		return smtpHost;
	}

	public void setSmtpHost(String smtpHost) {
		this.smtpHost = smtpHost;
	}

	public int getSmtpPort() {
		return smtpPort;
	}

	public void setSmtpPort(int smtpPort) {
		this.smtpPort = smtpPort;
	}

	public boolean isSmtpTls() {
		return smtpTls;
	}

	public void setSmtpTls(boolean smtpTls) {
		this.smtpTls = smtpTls;
	}

	public boolean isSmtpAuth() {
		return smtpAuth;
	}

	public void setSmtpAuth(boolean smtpAuth) {
		this.smtpAuth = smtpAuth;
	}

	public String getSmtpPassword() {
		return smtpPassword;
	}

	public void setSmtpPassword(String smtpPassword) {
		this.smtpPassword = smtpPassword;
	}

	public String getEmailFrom() {
		return emailFrom;
	}

	public void setEmailFrom(String emailFrom) {
		this.emailFrom = emailFrom;
	}

}
