
import javax.swing.*;
import java.awt.*;
import java.util.List;

public class ClientReaderGUI extends JFrame {
    private JTextArea textArea;

    public ClientReaderGUI() {
        super("Majority Lines Viewer");
        initializeGUI();
    }

    private void initializeGUI() {
        setSize(600, 400);
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        textArea = new JTextArea(20, 50);
        textArea.setEditable(false);
        add(new JScrollPane(textArea), BorderLayout.CENTER);

        setVisible(true);
    }
    public void displayLines(List<String> lines) {
        SwingUtilities.invokeLater(() -> textArea.setText(String.join("\n", lines)));
    }
}
