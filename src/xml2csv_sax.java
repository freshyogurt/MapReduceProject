import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class xml2csv_sax {

	public static void main(String[] argv) {

		try {

			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			PrintStream ps = new PrintStream(new BufferedOutputStream(
					new FileOutputStream(argv[1])));
			System.setOut(ps);
			final String[] attrNames = { "Id", "PostTypeId", "ParentId",
					"AcceptedAnswerId", "CreationDate" };

			DefaultHandler handler = new DefaultHandler() {

				public void startElement(String uri, String localName,
						String qName, Attributes attributes)
						throws SAXException {

					if ("row".equals(qName)) {
						StringBuilder sb = new StringBuilder();
						int index1 = 0;
						int index2 = 0;
						while(index1 < attrNames.length) {
							String attrName = attributes.getLocalName(index2);
							if (attrName.equals(attrNames[index1])) {
								sb.append(attributes.getValue(index2));
								index2++;
							}
							sb.append(",");
							index1++;
						}
						int length = sb.length();
						sb.deleteCharAt(length-1);
						System.out.print(sb);
					}
				}

				public void endElement(String uri, String localName,
						String qName) throws SAXException {
					if ("row".equals(qName)) {
						System.out.print("\n");
					}
				}
			};

			saxParser.parse(argv[0], handler);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}