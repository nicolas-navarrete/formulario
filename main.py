import numpy as np
import pandas as pd
import streamlit as st
import fitz # PyMuPDF
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import base64

conn = st.connection("snowflake")

def stylable_container(key, css_styles, wrapper_style=""):
    """
    Insert a container into your app which you can style using CSS.
    This is useful to style specific elements in your app.

    Args:
        key (str): The key associated with this container. This needs to be unique since all styles will be
            applied to the container with this key.
        css_styles (str | List[str]): The CSS styles to apply to the container elements.
            This can be a single CSS block or a list of CSS blocks.
        wrapper_style (str): (optional) Styles to apply to the wrapping container. Do not wrap in { }.

    Returns: A container object.
    """
    if isinstance(css_styles, str):
        css_styles = [css_styles]

    # Remove unneeded spacing that is added by the style markdown:
    css_styles.append(
        """
> div:first-child {
    display: none;
}
"""
    )

    style_text = """
<style>
"""
    if wrapper_style:
        style_text += f"""
    div[data-testid="stVerticalBlockBorderWrapper"]:has(
            > div
            > div[data-testid="stVerticalBlock"]
            > div.element-container
            > div.stMarkdown
            > div[data-testid="stMarkdownContainer"]
            > p
            > span.{key}
        ) {{
        {wrapper_style}
        }}
"""

    for style in css_styles:
        style_text += f"""

div[data-testid="stVerticalBlock"]:has(> div.element-container > div.stMarkdown > div[data-testid="stMarkdownContainer"] > p > span.{key}) {style}

"""

    style_text += f"""
    </style>

<span class="{key}"></span>
"""

    container = st.container()
    container.markdown(style_text, unsafe_allow_html=True)
    return container

class GridDeltaGenerator:
    def __init__(self, parent_dg, spec, *, gap="small", repeat=True):
        self._parent_dg = parent_dg
        self._container_queue = []
        self._number_of_rows = 0
        self._spec = spec
        self._gap = gap
        self._repeat = repeat

    def _get_next_cell_container(self):
        if not self._container_queue:
            if not self._repeat and self._number_of_rows > 0:
                raise StreamlitAPIException("The row is already filled up.")

            # Create a new row using st.columns:
            self._number_of_rows += 1
            spec = self._spec[self._number_of_rows % len(self._spec) - 1]
            self._container_queue.extend(self._parent_dg.columns(spec, gap=self._gap))

        return self._container_queue.pop(0)

    def __getattr__(self, name):
        return getattr(self._get_next_cell_container(), name)

def grid(*spec, gap="small", vertical_align="top"):
    """
    Insert a multi-element, grid container into your app.

    This function inserts a container into your app that arranges
    multiple elements in a grid layout as defined by the provided spec.
    Elements can be added to the returned container by calling methods directly
    on the returned object.

    Args:
        *spec (int | Iterable[int]): One or many row specs controlling the number and width of cells in each row.
            Each spec can be one of:
                * An integer specifying the number of cells. In this case, all cells have equal
                width.
                * An iterable of numbers (int or float) specifying the relative width of
                each cell. E.g., ``[0.7, 0.3]`` creates two cells, the first
                one occupying 70% of the available width and the second one 30%.
                Or, ``[1, 2, 3]`` creates three cells where the second one is twice
                as wide as the first one, and the third one is three times that width.
                The function iterates over the provided specs in a round-robin order. Upon filling a row,
                it moves on to the next spec, or the first spec if there are no
                more specs.
        gap (Optional[str], optional): The size of the gap between cells, specified as "small", "medium", or "large".
            This parameter defines the visual space between grid cells. Defaults to "small".
        vertical_align (Literal["top", "center", "bottom"], optional): The vertical alignment of the cells in the row.
            Defaults to "top".
    """

    container = stylable_container(
        key=f"grid_{vertical_align}",
        css_styles=[
            """
div[data-testid="column"] > div[data-testid="stVerticalBlockBorderWrapper"] > div {
height: 100%;
}
""",
            """
div[data-testid="column"] > div {
height: 100%;
}
""",
            f"""
div[data-testid="column"] > div[data-testid="stVerticalBlockBorderWrapper"] > div > div[data-testid="stVerticalBlock"] > div.element-container {{
    {"margin-top: auto;" if vertical_align in ["center", "bottom"] else ""}
    {"margin-bottom: auto;" if vertical_align == "center" else ""}
}}
""",
            f"""
div[data-testid="column"] > div > div[data-testid="stVerticalBlock"] > div.element-container {{
    {"margin-top: auto;" if vertical_align in ["center", "bottom"] else ""}
    {"margin-bottom: auto;" if vertical_align == "center" else ""}
}}
""",
        ],
    )

    return GridDeltaGenerator(
        parent_dg=container, spec=list(spec), gap=gap, repeat=True
    )

def extract_pdf_text(file_path):
    """
    Extrae el texto de un archivo PDF.

    Args:
        file_path (str): Ruta al archivo PDF.

    Returns:
        str: Texto extraído del PDF.
    """
    pdf_text = ""
    with fitz.open(file_path) as pdf_document:
        for page in pdf_document:
            pdf_text += page.get_text()

    return pdf_text

def display_pdf_as_text(file_path):
    """
    Extrae y muestra el texto de un archivo PDF.

    Args:
        file_path (str): Ruta al archivo PDF.
    """
    pdf_text = extract_pdf_text(file_path)
    st.text_area("", pdf_text, height=500)

def formulario():
    with stylable_container(
        key="title",
        css_styles="""
        {
            p {
                font-size: 16px;
                font-weight: bold;
                text-align: center;
                font-family: sans-serif;
                border: 1px solid #2eaa4f;
                border-radius: 0.5rem;
            }
        }
            """,
    ):
        st.write("Consentimiento")
        
    with stylable_container(
        key="form",
        css_styles="""
        {
            .st-emotion-cache-qcpnpn {
                font-family: sans-serif;
                border: 1px solid #2eaa4f; 
            }

            .st-emotion-cache-4uzi61 {
                font-family: sans-serif;
                border: 1px solid #2eaa4f; 
            }

            p {
                font-size: 14px;
                font-weight: bold;
            }
        }
            """,
    ):
        with st.form("form", clear_on_submit=True):
            my_grid = grid(1, 2, 2, 1, 1, vertical_align="bottom")

            nombre = my_grid.text_input("Nombre y Apellido:", "", key="nombre")
            rut = my_grid.text_input("RUT:", "", key="rut")
            llave_unica = my_grid.text_input("DV (digito verificador):", "", key="llave_unica")
            correo = my_grid.text_input("Correo Electrónico:", "", key="correo")
            telefono = my_grid.text_input("Número de Teléfono:", "", key="telefono")
            preferencias = my_grid.selectbox("Preferencias de Contacto:", ["Email", "Teléfono", "WhatsApp"], key="preferencias")

            with st.container():
                with st.expander("Términos y Condiciones"):
                    # Muestra el texto del PDF
                    display_pdf_as_text(r"files/Plataforma Administración Consentimiento.pdf")

            with stylable_container(
                key="botonForm",
                css_styles="""
                {
                    p {
                        font-size: 14px;  
                    }
                }
                    """,
            ):
                # Checkbox para habilitar el botón
                checkbox = st.checkbox("Acepto los términos y condiciones", key="checkbox")

                # Botón de envío
                submitted = st.form_submit_button("Enviar")

                with stylable_container(
                    key="result",
                    css_styles="""
                    {
                        p {
                            font-size: 14px;  
                        }
                    }
                        """,
                ):
                    if submitted and checkbox and nombre and rut and llave_unica and correo and telefono and preferencias:
                        st.success("Formulario enviado correctamente.")

                        # Obtener la fecha actual
                        fecha_aceptacion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        # Insertar los datos en la base de datos
                        insert = """
                        INSERT INTO DEMOS_DB.RAW.RESPUESTAS (NOMBRE, RUT, DV, CORREO, TELEFONO, PREFERENCIAS_CONTACTO, FECHA_ACEPTACION)
                            VALUES(
                            '{NOMBRE}',
                            '{RUT}',
                            '{DV}',
                            '{CORREO}',
                            '{TELEFONO}',
                            '{PREFERENCIAS_CONTACTO}',
                            '{FECHA_ACEPTACION}'
                            )
                        """
                        format_sql = insert.format(
                            NOMBRE=nombre, 
                            RUT=rut, 
                            DV=llave_unica,
                            CORREO=correo,
                            TELEFONO=telefono,
                            PREFERENCIAS_CONTACTO=preferencias,
                            FECHA_ACEPTACION=fecha_aceptacion
                        )
                        
                        conn.session().sql(format_sql).collect()
                    elif not checkbox:
                        st.error("Debe aceptar los términos y condiciones antes de enviar el formulario.")
                    elif submitted:
                        st.error("Debe completar todas las preguntas antes de enviar el formulario.")

formulario()