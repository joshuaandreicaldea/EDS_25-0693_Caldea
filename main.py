import pandas as pd
import numpy as np
import plotly.express as px
import os

class BeamDataPipeline:
    def __init__(self, file_path):
        self.file_path = file_path
        self.raw_data = None
        self.cleaned_data = None
        
    def ingest_data(self):
        """Module 1: Robust Data Ingestion"""
        try:
            self.raw_data = pd.read_csv(self.file_path)
            print("✅ Data ingested successfully.")
        except FileNotFoundError:
            print("❌ Error: CSV file not found.")
        except Exception as e:
            print(f"❌ Error: {e}")

    def clean_and_filter(self):
        """Module 2 & 3: Data Cleaning and Unique Filtering"""
        if self.raw_data is not None:
            df = self.raw_data.copy()
            df = df.dropna()
            df = df.drop_duplicates()
            
            #deep beams
            self.cleaned_data = df[df['Beam_Height_mm'] > 450].copy()
            
            os.makedirs('data', exist_ok=True)
            self.cleaned_data.to_csv('data/dataset_cleaned.csv', index=False)
            print(f"✅ Data cleaned. Shape: {self.cleaned_data.shape}")

    def run_statistics(self):
        """Module 4: Engineering Data Analytics using NumPy"""
        if self.cleaned_data is not None:
            strength = self.cleaned_data['Bearing_Capacity_kN'].values 
            metrics = {
                "Mean": np.mean(strength),
                "Median": np.median(strength),
                "Std Dev": np.std(strength),
                "Variance": np.var(strength)
            }
            print("\n--- Engineering Metrics ---")
            for key, val in metrics.items():
                print(f"{key}: {val:.2f}")
            return metrics

    def generate_visuals(self):
        """Module 5: Visualization & Animation (PDF Requirements)"""
        if self.cleaned_data is not None:
            os.makedirs('outputs', exist_ok=True) 
            
            #STATIC GRAPHS
            # 1. Histogram
            fig1 = px.histogram(self.cleaned_data, x="Bearing_Capacity_kN", title="Static 1: Capacity Distribution")
            fig1.write_image("outputs/static1_histogram.png")
            
            # 2. Boxplot
            fig2 = px.box(self.cleaned_data, x="Concrete_Grade_MPa", y="Bearing_Capacity_kN", title="Static 2: Capacity Variance")
            fig2.write_image("outputs/static2_boxplot.png")
            
            # 3. Scatter
            fig3 = px.scatter(self.cleaned_data, x="Longitudinal_Reinforcement_Ratio_percent", y="Bearing_Capacity_kN", title="Static 3: Reinforcement vs Capacity")
            fig3.write_image("outputs/static3_scatter.png")
            
            #ANIMATED GRAPHS
            df_anim = self.cleaned_data.sort_values(by="Concrete_Grade_MPa") 
            
            # 1. Animated Scatter
            anim1 = px.scatter(df_anim, x="Longitudinal_Reinforcement_Ratio_percent", y="Bearing_Capacity_kN", 
                               animation_frame="Concrete_Grade_MPa", title="Animated 1: Capacity Dynamics")
            anim1.write_html("outputs/animated1_scatter.html")

            # 2. Animated Histogram
            anim2 = px.histogram(df_anim, x="Bearing_Capacity_kN", 
                                 animation_frame="Concrete_Grade_MPa", title="Animated 2: Distribution Shift")
            anim2.write_html("outputs/animated2_histogram.html")
            
            print("✅ All 5 visuals saved to outputs/ folder.")

if __name__ == "__main__":
    pipeline = BeamDataPipeline('data/rc_beam_bearing_capacity_dataset.csv')
    pipeline.ingest_data()
    pipeline.clean_and_filter()
    pipeline.run_statistics()
    pipeline.generate_visuals()